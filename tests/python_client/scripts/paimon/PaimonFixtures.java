/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Base64;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.stream.Stream;

/** Offline-only writer AND independent batch reader. No JVM is needed by pytest. */
public final class PaimonFixtures {
    private static final LocalFileIO IO = LocalFileIO.create();
    private static final int DIM = 128;
    private static boolean wide;

    private static float[] vector(long id, int epoch) {
        float[] values = new float[DIM];
        for (int j = 0; j < DIM; j++) {
            long x = (id + 1) * 0x9e3779b9L + (j + 1) * 0x85ebca6bL + epoch * 0xc2b2ae35L;
            x ^= x >>> 16;
            values[j] = ((x & 2047) - 1023.5f) / 1024;
        }
        return values;
    }

    private static GenericRow row(long id, int epoch, RowKind kind) {
        GenericRow row = GenericRow.of(
                id, id % 7 == 0 ? null : BinaryString.fromString("label_" + id + "_" + epoch),
                id * 1.25 + epoch, new GenericArray(vector(id, epoch)),
                id * 0.01 + epoch, new GenericArray(vector(id, epoch + 7)),
                BinaryString.fromString("document topic " + (id % 5) + " record " + id + " epoch " + epoch),
                (int) (id % 5));
        row.setRowKind(kind);
        if (wide) {
            GenericRow extended = new GenericRow(24);
            for (int field = 0; field < 8; field++) {
                extended.setField(field, row.getField(field));
            }
            extended.setField(1, id % 100 == 0 ? null : BinaryString.fromString(id == 1 ? "中文 café 🚀" : id == 2 ? "" : "label_" + id));
            extended.setField(8, id % 5 == 0 ? null : id % 2 == 0);
            extended.setField(9, id < 2 ? (id == 0 ? Byte.MIN_VALUE : Byte.MAX_VALUE) : (byte) (id % 255 - 128));
            extended.setField(10, id < 2 ? (id == 0 ? Short.MIN_VALUE : Short.MAX_VALUE) : (short) (id % 65535 - 32768));
            extended.setField(11, id < 2 ? (id == 0 ? Integer.MIN_VALUE : Integer.MAX_VALUE) : (int) id * 7);
            extended.setField(12, id % 5 == 0 ? null : (float) (id * 1.5));
            extended.setField(13, BinaryString.fromString("{\"k\":" + id + ",\"g\":" + id % 3 + "}"));
            extended.setField(14, id % 11 == 0 ? null : new GenericArray(id == 2 ? new long[]{} : new long[]{id, id + 1, id + 2}));
            extended.setField(15, Timestamp.fromEpochMillis(1700000000000L + id * 1000, (int) (id * 137 % 1000) * 1000));
            byte[] binary = new byte[DIM / 8];
            for (int j = 0; j < binary.length; j++) binary[j] = (byte) ((id * 31 + j * 7) & 255);
            extended.setField(16, binary);
            byte[] int8 = new byte[DIM];
            for (int j = 0; j < DIM; j++) int8[j] = (byte) ((id + j) % 127 - 63);
            extended.setField(17, int8);
            Map<Long, Double> sparse = new LinkedHashMap<>();
            sparse.put(3L, 0.5);
            sparse.put(17L, (double) (id + 1));
            extended.setField(18, new GenericMap(sparse));
            extended.setField(19, null);
            ByteBuffer half = ByteBuffer.allocate(DIM * 2).order(ByteOrder.LITTLE_ENDIAN);
            ByteBuffer bf16 = ByteBuffer.allocate(DIM * 2).order(ByteOrder.LITTLE_ENDIAN);
            for (float value : vector(id, epoch)) {
                // Fixture values are finite normal numbers in [-1,1], >= 2^-11 in magnitude.
                int bits = Float.floatToIntBits(value);
                int roundedHalf = bits + 0xfff + ((bits >>> 13) & 1);
                half.putShort((short) (((roundedHalf >>> 16) & 0x8000) | (((roundedHalf >>> 23) - 112) << 10) | ((roundedHalf >>> 13) & 0x3ff)));
                bf16.putShort((short) ((bits + 0x7fff + ((bits >>> 16) & 1)) >>> 16));
            }
            extended.setField(20, half.array());
            extended.setField(21, bf16.array());
            extended.setField(22, id == 0 ? Long.MIN_VALUE : id == 1 ? Long.MAX_VALUE : id);
            extended.setField(23, BinaryString.fromString("POINT(" + id + " " + (id + 1) + ")"));
            return extended;
        }
        return row;
    }

    private static FileStoreTable create(Path path, String mode, int buckets) throws Exception {
        Schema.Builder schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("label", DataTypes.STRING())
                .column("value", DataTypes.DOUBLE())
                .column("embedding", DataTypes.ARRAY(DataTypes.FLOAT().notNull()))
                .column("score", DataTypes.DOUBLE())
                .column("new_embedding", DataTypes.ARRAY(DataTypes.FLOAT().notNull()))
                .column("doc", DataTypes.STRING())
                .column("group_id", DataTypes.INT())
                .option("file.format", "parquet")
                .option("snapshot.num-retained.min", "100")
                .option("snapshot.num-retained.max", "100")
                .option("write-buffer-size", "16 mb")
                .option("target-file-size", "2 mb")
                .option("source.split.target-size", "1 b")
                .option("source.split.open-file-cost", "1 b");
        if (wide) {
            schema.column("bool_value", DataTypes.BOOLEAN()).column("int8_value", DataTypes.TINYINT())
                    .column("int16_value", DataTypes.SMALLINT()).column("int32_value", DataTypes.INT())
                    .column("float_value", DataTypes.FLOAT()).column("json_value", DataTypes.STRING())
                    .column("arr", DataTypes.ARRAY(DataTypes.BIGINT().notNull()))
                    .column("ts", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6))
                    .column("binary_vec", DataTypes.BYTES()).column("int8_vec", DataTypes.BYTES())
                    .column("sparse", DataTypes.MAP(DataTypes.BIGINT().notNull(), DataTypes.DOUBLE().notNull()))
                    .column("all_null", DataTypes.DOUBLE())
                    .column("half_vec", DataTypes.BYTES()).column("bf16_vec", DataTypes.BYTES())
                    .column("long_value", DataTypes.BIGINT()).column("geo", DataTypes.STRING());
        }
        if (!mode.equals("append") && !mode.equals("mixed") && !mode.equals("partitioned") && !wide) {
            schema.primaryKey("id").option("bucket", Integer.toString(buckets));
        }
        if (mode.equals("partitioned")) {
            schema.partitionKeys("group_id");
        }
        if (mode.equals("dv") || mode.equals("bitmap64")) {
            schema.option("deletion-vectors.enabled", "true")
                    .option("deletion-vectors.bitmap64", mode.equals("bitmap64") ? "true" : "false");
        }
        org.apache.paimon.fs.Path tablePath = new org.apache.paimon.fs.Path(path.toUri());
        new SchemaManager(IO, tablePath).createTable(schema.build());
        return FileStoreTableFactory.create(IO, tablePath);
    }

    private static void commit(FileStoreTable table, List<GenericRow> rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (IOManager io = IOManager.create(System.getProperty("java.io.tmpdir"));
             BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
            write.withIOManager(io);
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private static List<Map<String, Object>> read(FileStoreTable table, long snapshot) throws Exception {
        FileStoreTable pinned = table.copy(Collections.singletonMap("scan.snapshot-id", Long.toString(snapshot)));
        ReadBuilder builder = pinned.newReadBuilder();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Split split : builder.newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader = builder.newRead().createReader(split)) {
                reader.forEachRemaining(row -> {
                    Map<String, Object> values = new LinkedHashMap<>();
                    for (int index = 0; index < pinned.schema().fields().size(); index++) {
                        var field = pinned.schema().fields().get(index);
                        Object value = InternalRow.createFieldGetter(field.type(), index).getFieldOrNull(row);
                        values.put(field.name(), convert(field.type(), value));
                    }
                    rows.add(values);
                });
            }
        }
        rows.sort(Comparator.comparingLong(row -> ((Number) row.get("id")).longValue()));
        return rows;
    }

    private static Object convert(DataType type, Object value) {
        if (value == null) return null;
        if (value instanceof BinaryString) return value.toString();
        if (value instanceof byte[]) return Collections.singletonMap("__bytes__", Base64.getEncoder().encodeToString((byte[]) value));
        if (value instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) value;
            return Collections.singletonMap("__timestamp_micros__", timestamp.getMillisecond() * 1000 + timestamp.getNanoOfMillisecond() / 1000);
        }
        if (value instanceof InternalArray) {
            InternalArray array = (InternalArray) value;
            DataType elementType = ((ArrayType) type).getElementType();
            var getter = InternalArray.createElementGetter(elementType);
            List<Object> output = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) output.add(convert(elementType, getter.getElementOrNull(array, i)));
            return output;
        }
        if (value instanceof InternalMap) {
            InternalMap map = (InternalMap) value;
            MapType mapType = (MapType) type;
            var keys = InternalArray.createElementGetter(mapType.getKeyType());
            var values = InternalArray.createElementGetter(mapType.getValueType());
            Map<String, Object> output = new LinkedHashMap<>();
            for (int i = 0; i < map.size(); i++) output.put(keys.getElementOrNull(map.keyArray(), i).toString(), convert(mapType.getValueType(), values.getElementOrNull(map.valueArray(), i)));
            return output;
        }
        return value;
    }

    private static void capture(Path directory, FileStoreTable table, Path output, String name) throws Exception {
        long snapshot = table.latestSnapshot().get().id();
        List<Map<String, Object>> rows = read(table, snapshot);
        Path image = output.resolve(name);
        Files.createDirectories(image.resolve("table"));
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : (Iterable<Path>) paths.filter(Files::isRegularFile)::iterator) {
                Path target = image.resolve("table").resolve(directory.relativize(path));
                Files.createDirectories(target.getParent());
                Files.copy(path, target);
            }
        }
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("snapshot_id", snapshot);
        metadata.put("dimension", DIM);
        metadata.put("writer", "org.apache.paimon:paimon-bundle:1.2.0");
        metadata.put("reader", "Paimon Java 1.2.0 pinned batch reader");
        metadata.put("generator_sha256", System.getProperty("paimon.fixture.generator.sha256"));
        metadata.put("java_version", System.getProperty("java.version"));
        metadata.put("options", table.options());
        metadata.put("rows", rows);
        List<Map<String, Object>> splits = new ArrayList<>();
        for (Split sourceSplit : table.copy(Collections.singletonMap("scan.snapshot-id", Long.toString(snapshot)))
                .newReadBuilder().newScan().plan().splits()) {
            DataSplit split = (DataSplit) sourceSplit;
            Map<String, Object> planned = new LinkedHashMap<>();
            planned.put("bucket", split.bucket());
            planned.put("raw_convertible", split.rawConvertible());
            planned.put("schema_ids", split.dataFiles().stream().map(file -> file.schemaId()).toArray());
            splits.add(planned);
        }
        metadata.put("java_planned_splits", splits);
        Files.write(image.resolve("oracle.json"), JsonSerdeUtil.toJson(metadata).getBytes(StandardCharsets.UTF_8));
        System.out.println(name + ": snapshot=" + snapshot + ", rows=" + rows.size());
    }

    public static void main(String[] args) throws Exception {
        Path output = Path.of(args[0]).toAbsolutePath();
        String mode = args[1];
        wide = mode.equals("wide");
        int count = Integer.parseInt(args[2]);
        int buckets = args.length > 3 ? Integer.parseInt(args[3]) : 1;
        Files.createDirectories(output);
        Path source = output.resolve("source");
        FileStoreTable table = create(source, mode, buckets);
        List<GenericRow> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            rows.add(row(i, 0, RowKind.INSERT));
        }
        commit(table, rows);
        capture(source, table, output, "s1");
        rows.clear();
        if (mode.equals("mixed")) {
            org.apache.paimon.fs.Path tablePath = new org.apache.paimon.fs.Path(source.toUri());
            new SchemaManager(IO, tablePath).commitChanges(SchemaChange.addColumn("evolved", DataTypes.STRING()));
            table = FileStoreTableFactory.create(IO, tablePath);
            for (int i = count; i < count + Math.max(1, count / 10); i++) {
                GenericRow original = row(i, 1, RowKind.INSERT);
                GenericRow extended = new GenericRow(9);
                for (int field = 0; field < 8; field++) {
                    extended.setField(field, original.getField(field));
                }
                extended.setField(8, BinaryString.fromString("new schema"));
                rows.add(extended);
            }
        } else if (mode.equals("append") || mode.equals("partitioned") || wide) {
            for (int i = count; i < count + Math.max(1, count / 10); i++) {
                rows.add(row(i, 1, RowKind.INSERT));
            }
        } else {
            for (int i = 0; i < Math.max(1, count / 6); i++) {
                rows.add(row(i, 0, RowKind.DELETE));
                rows.add(row(count / 2 + i, 1, RowKind.INSERT));
                rows.add(row(count + i, 1, RowKind.INSERT));
            }
        }
        commit(table, rows);
        capture(source, table, output, "s2");
        if (mode.equals("mor")) {
            rows.clear();
            rows.add(row(0, 2, RowKind.INSERT)); // Reinsert a deleted key.
            rows.add(row(count / 2, 2, RowKind.INSERT)); // Update the same key again.
            commit(table, rows);
            capture(source, table, output, "s3");
        }
        if (!mode.equals("append") && !mode.equals("mixed") && !mode.equals("partitioned") && !wide) {
            rows.clear();
            for (Map<String, Object> current : read(table, table.latestSnapshot().get().id())) {
                rows.add(row(((Number) current.get("id")).longValue(), 0, RowKind.DELETE));
            }
            commit(table, rows);
            capture(source, table, output, "empty");
        }
    }
}
