/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A field added to a struct that is a map value is missing from files written earlier. Projecting
 * only that field must still read the map and return null, including when the value struct itself
 * is null.
 */
class TestMapStructSchemaEvolution {
  @TempDir private File temp;

  @Test
  public void addedFieldInMapValue() throws IOException {
    Schema writeSchema = schema(false);
    File file = write(writeSchema, rows(writeSchema));
    Schema projection = schema(true).select("page_id", "slots.origin");

    List<Record> records = read(file, projection);
    assertThat(records).hasSize(4);

    Record first = records.get(0);
    assertThat(first.getField("page_id")).isEqualTo(12345L);
    Map<String, ?> firstSlots = map(first.getField("slots"));
    assertThat(firstSlots).containsOnlyKeys("main", "talk");
    assertThat(struct(firstSlots.get("main")).getField("origin")).isNull();
    assertThat(firstSlots.get("talk")).isNull();

    assertThat(records.get(1).getField("page_id")).isEqualTo(999L);
    assertThat(records.get(1).getField("slots")).isNull();

    assertThat(records.get(2).getField("page_id")).isEqualTo(7L);
    assertThat(map(records.get(2).getField("slots"))).isEmpty();

    Map<String, ?> mixed = map(records.get(3).getField("slots"));
    assertThat(mixed).containsOnlyKeys("a", "b", "c");
    assertThat(struct(mixed.get("a")).getField("origin")).isNull();
    assertThat(mixed.get("b")).isNull();
    assertThat(struct(mixed.get("c")).getField("origin")).isNull();
  }

  @Test
  public void addedFieldInsideNestedMapValue() throws IOException {
    Types.StructType writtenInner =
        Types.StructType.of(
            Types.NestedField.optional(6, "body", Types.StringType.get()),
            Types.NestedField.optional(7, "format", Types.StringType.get()));
    Types.StructType writtenValue =
        Types.StructType.of(Types.NestedField.optional(5, "content", writtenInner));
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(1, "page_id", Types.LongType.get()),
            Types.NestedField.optional(
                2, "slots", Types.MapType.ofOptional(3, 4, Types.StringType.get(), writtenValue)));

    Record content = GenericRecord.create(writtenInner);
    content.setField("body", "hello");
    content.setField("format", "text/x-wiki");
    Record value = GenericRecord.create(writtenValue);
    value.setField("content", content);
    Record row = GenericRecord.create(writeSchema);
    row.setField("page_id", 12345L);
    Map<String, Record> slots = Maps.newLinkedHashMap();
    slots.put("main", value);
    slots.put("empty", null);
    row.setField("slots", slots);

    Record nullRow = GenericRecord.create(writeSchema);
    nullRow.setField("page_id", 1L);
    nullRow.setField("slots", null);

    File file = write(writeSchema, List.of(row, nullRow));

    Types.StructType readInner =
        Types.StructType.of(Types.NestedField.optional(8, "origin", Types.LongType.get()));
    Schema projection =
        new Schema(
            Types.NestedField.required(1, "page_id", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "slots",
                Types.MapType.ofOptional(
                    3,
                    4,
                    Types.StringType.get(),
                    Types.StructType.of(Types.NestedField.optional(5, "content", readInner)))));

    List<Record> records = read(file, projection);
    assertThat(records).hasSize(2);
    Map<String, ?> readSlots = map(records.get(0).getField("slots"));
    assertThat(readSlots).containsOnlyKeys("main", "empty");
    assertThat(struct(struct(readSlots.get("main")).getField("content")).getField("origin"))
        .isNull();
    assertThat(readSlots.get("empty")).isNull();
    assertThat(records.get(1).getField("slots")).isNull();
  }

  private static Schema schema(boolean withOrigin) {
    List<Types.NestedField> valueFields =
        Lists.newArrayList(
            Types.NestedField.optional(5, "body", Types.StringType.get()),
            Types.NestedField.optional(6, "format", Types.StringType.get()));
    if (withOrigin) {
      valueFields.add(Types.NestedField.optional(7, "origin", Types.LongType.get()));
    }

    return new Schema(
        Types.NestedField.required(1, "page_id", Types.LongType.get()),
        Types.NestedField.optional(
            2,
            "slots",
            Types.MapType.ofOptional(
                3, 4, Types.StringType.get(), Types.StructType.of(valueFields))));
  }

  private static List<Record> rows(Schema writeSchema) {
    Types.StructType valueType =
        writeSchema.findType("slots").asMapType().valueType().asStructType();

    Record main = GenericRecord.create(valueType);
    main.setField("body", "This is the main content of the page");
    main.setField("format", "text/x-wiki");
    Record first = GenericRecord.create(writeSchema);
    first.setField("page_id", 12345L);
    Map<String, Record> firstSlots = Maps.newLinkedHashMap();
    firstSlots.put("main", main);
    firstSlots.put("talk", null);
    first.setField("slots", firstSlots);

    Record nullMap = GenericRecord.create(writeSchema);
    nullMap.setField("page_id", 999L);
    nullMap.setField("slots", null);

    Record emptyMap = GenericRecord.create(writeSchema);
    emptyMap.setField("page_id", 7L);
    emptyMap.setField("slots", Maps.newLinkedHashMap());

    Record present = GenericRecord.create(valueType);
    present.setField("body", "a");
    present.setField("format", "text");
    Record other = GenericRecord.create(valueType);
    other.setField("body", "c");
    other.setField("format", null);
    Record mixed = GenericRecord.create(writeSchema);
    mixed.setField("page_id", 8L);
    Map<String, Record> mixedSlots = Maps.newLinkedHashMap();
    mixedSlots.put("a", present);
    mixedSlots.put("b", null);
    mixedSlots.put("c", other);
    mixed.setField("slots", mixedSlots);

    return List.of(first, nullMap, emptyMap, mixed);
  }

  private File write(Schema writeSchema, List<Record> records) throws IOException {
    File file = new File(temp, "map-struct-" + System.nanoTime() + ".parquet");
    try (FileAppender<Record> appender =
        Parquet.write(Files.localOutput(file))
            .schema(writeSchema)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      appender.addAll(records);
    }

    return file;
  }

  private static List<Record> read(File file, Schema projection) throws IOException {
    try (CloseableIterable<Record> records =
        Parquet.read(Files.localInput(file))
            .project(projection)
            .reuseContainers()
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema))
            .build()) {
      // copy before the reader reuses the row, including map entries the record copy shares
      List<Record> copied = Lists.newArrayList();
      for (Record record : records) {
        Record copy = ((GenericRecord) record).copy();
        Object slots = record.getField("slots");
        if (slots instanceof Map<?, ?> map) {
          Map<String, Object> slotsCopy = Maps.newLinkedHashMap();
          for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof GenericRecord generic) {
              value = generic.copy();
            }

            slotsCopy.put((String) entry.getKey(), value);
          }

          copy.setField("slots", slotsCopy);
        }

        copied.add(copy);
      }

      return copied;
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, ?> map(Object value) {
    return (Map<String, ?>) value;
  }

  private static Record struct(Object value) {
    return (Record) value;
  }
}
