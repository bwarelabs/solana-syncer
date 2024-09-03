package com.bwarelabs;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.serializer.WritableSerialization;

public class CosToHbaseWriter {

    private static final Logger logger = Logger.getLogger(CosToHbaseWriter.class.getName());

    private final String START_KEY;
    private final String END_KEY;
    private final String WORKSPACE;

    private final Configuration CONFIG_HBASE;
    private final Configuration CONFIG_HADOOP;

    public CosToHbaseWriter(Properties properties, String startKey, String endKey) throws Exception {
        LogManager.getLogManager().readConfiguration(
                CosToHbaseWriter.class.getClassLoader().getResourceAsStream("logging.properties"));

        this.START_KEY = startKey != null ? startKey : Utils.getRequiredProperty(properties, "hbase.start-key");
        this.END_KEY = endKey != null ? endKey : Utils.getRequiredProperty(properties, "hbase.end-key");
        this.WORKSPACE = Utils.getRequiredProperty(properties, "hbase.download-dir");

        // HBase configuration
        CONFIG_HBASE = HBaseConfiguration.create();
        CONFIG_HBASE.set("hbase.zookeeper.quorum", Utils.getRequiredProperty(properties, "hbase.zookeeper.quorum"));
        CONFIG_HBASE.set("hbase.zookeeper.property.clientPort",
                Utils.getRequiredProperty(properties, "hbase.zookeeper.property.clientPort"));
        CONFIG_HBASE.set("hbase.client.keyvalue.maxsize",
                Utils.getRequiredProperty(properties, "hbase.client.keyvalue.maxsize"));

        CONFIG_HADOOP = new Configuration();
        CONFIG_HADOOP.setStrings("io.serializations", ResultSerialization.class.getName(),
                WritableSerialization.class.getName());

    }

    public void write() throws Exception {
        String regex = "range_([0-9a-fA-F]+)";
        Pattern pattern = Pattern.compile(regex);

        int end_boundary = Integer.parseInt(END_KEY, 16);
        int start_boundary = Integer.parseInt(START_KEY, 16);

        try (Connection connection = ConnectionFactory.createConnection(CONFIG_HBASE);) {
            createTables(connection);

            CosUtils.walkKeysByPrefix((key) -> {
                Matcher matcher = pattern.matcher(key);
                if (!matcher.find()) {
                    return;
                }
                String start = matcher.group(1);
                int start_value = Integer.parseInt(start, 16);
                if (start_value < start_boundary || start_value >= end_boundary) {
                    return;
                }
                if (key.contains("blocks")) {
                    processBlock(connection, key);
                }
                if (key.contains("entries")) {
                    processEntries(connection, key);
                }
            });
        }
    }

    private void createTables(Connection conn) throws IOException {
        var requiredTables = new String[] { "blocks", "entries", "tx", "tx-by-addr", "snapshots" };
        var columnFamilyName = "x";

        var columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyName.getBytes()).build();
        for (var table : requiredTables) {
            var tableName = TableName.valueOf(table);
            if (!conn.getAdmin().tableExists(tableName)) {
                var tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(columnFamily)
                        .build();
                conn.getAdmin().createTable(tableDescriptor);
            }
        }
    }

    private boolean hasKey(Connection conn, String tbl, String cosKey) {
        try (Table blocks_status = conn.getTable(TableName.valueOf(tbl))) {
            Get get = new Get(cosKey.getBytes());
            Result result = blocks_status.get(get);
            return !result.isEmpty();
        } catch (IOException e) {
            logger.severe("Error checking key: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private void processBlock(Connection conn, String cosKey) {
        logger.info("Checking block range completion: " + cosKey);
        if (hasKey(conn, "snapshots", cosKey)) {
            logger.info("Block range already processed: " + cosKey);
            return;
        }

        logger.info("Downloading blocks in range: " + cosKey);
        String dstPath = WORKSPACE + "/" + cosKey;
        try {
            CosUtils.downloadByKey(cosKey, dstPath);

            logger.info("Processing blocks range: " + dstPath);
            Path blockFile = new Path(dstPath);
            try (SequenceFile.Reader reader = new SequenceFile.Reader(CONFIG_HADOOP,
                    SequenceFile.Reader.file(blockFile))) {
                ImmutableBytesWritable key = (ImmutableBytesWritable) reader.getKeyClass().getDeclaredConstructor()
                        .newInstance();

                try (Table blocks = conn.getTable(TableName.valueOf("blocks"));
                        Table blocksStatus = conn.getTable(TableName.valueOf("snapshots"))) {
                    Result result = null;
                    while (reader.next(key)) {
                        result = (Result) reader.getCurrentValue(result);

                        String rowKey = new String(key.get(), 0, key.getLength());

                        // logger.info("Processing block: " + rowKey + " value size MB: "
                        // + result.value().length);

                        Put put = new Put(Bytes.toBytes(rowKey));
                        put.addColumn(Bytes.toBytes("x"), result.rawCells()[0].getQualifierArray(), result.value());

                        blocks.put(put);

                        // Prepare tx and tx-by-addr
                        try (BufferedMutator tx = conn.getBufferedMutator(TableName.valueOf("tx"));
                                BufferedMutator txByAddr = conn.getBufferedMutator(TableName.valueOf("tx-by-addr"))) {
                            BigtableBlock block = new BigtableBlock(rowKey, result.rawCells()[0].getQualifierArray(),
                                    result.value());
                            // logger.info(rowKey + " " + block.txs.size() + " " + block.txByAddrs.size());
                            for (BigtableCell cell : block.txs) {
                                Put txPut = new Put(cell.key().getBytes());
                                txPut.addColumn("x".getBytes(), "bin".getBytes(), cell.value());
                                tx.mutate(txPut);
                            }
                            for (BigtableCell cell : block.txByAddrs) {
                                Put txByAddrPut = new Put(cell.key().getBytes());
                                txByAddrPut.addColumn("x".getBytes(), "proto".getBytes(), cell.value());
                                txByAddr.mutate(txByAddrPut);
                            }
                        }
                    }
                    // Insert marker for block completion
                    Put put_status = new Put(Bytes.toBytes(cosKey));
                    put_status.addColumn("x".getBytes(), "status".getBytes(), new byte[] {});

                    blocksStatus.put(put_status);
                }
            } catch (IOException | InstantiationException | IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                logger.severe("Error processing blocks file: " + e.getMessage());
                e.printStackTrace();
            }
        } finally {
            // TODO: refactor into try-with-resources
            File file = new File(dstPath);
            if (file.exists()) {
                file.delete();
            }
        }
    }

    private void processEntries(Connection conn, String cosKey) {
        logger.info("Checking entries range completion: " + cosKey);
        if (hasKey(conn, "snapshots", cosKey)) {
            logger.info("Entries range already processed: " + cosKey);
            return;
        }

        logger.info("Downloading entries: " + cosKey);
        String dstPath = WORKSPACE + "/" + cosKey;
        try {
            CosUtils.downloadByKey(cosKey, dstPath);

            logger.info("Processing entries in range: " + dstPath);
            Path entriesFile = new Path(dstPath);
            try (SequenceFile.Reader reader = new SequenceFile.Reader(CONFIG_HADOOP,
                    SequenceFile.Reader.file(entriesFile))) {
                ImmutableBytesWritable key = (ImmutableBytesWritable) reader.getKeyClass().getDeclaredConstructor()
                        .newInstance();

                try (BufferedMutator entries = conn.getBufferedMutator(TableName.valueOf("entries"));
                        BufferedMutator entriesStatus = conn.getBufferedMutator(TableName.valueOf("snapshots"))) {
                    Result result = null;
                    while (reader.next(key)) {
                        result = (Result) reader.getCurrentValue(result);

                        String rowKey = new String(key.get(), 0, key.getLength());

                        Put put = new Put(Bytes.toBytes(rowKey));
                        put.addColumn(Bytes.toBytes("x"), result.rawCells()[0].getQualifierArray(), result.value());

                        entries.mutate(put);
                    }
                    // Insert marker for entries completion
                    Put put_status = new Put(Bytes.toBytes(cosKey));
                    put_status.addColumn("x".getBytes(), "status".getBytes(), new byte[] {});

                    entriesStatus.mutate(put_status);
                }
            } catch (IOException | InstantiationException | IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                logger.severe("Error processing entries file: " + e.getMessage());
                e.printStackTrace();
            }
        } finally {
            // TODO: refactor into try-with-resources
            File file = new File(dstPath);
            if (file.exists()) {
                file.delete();
            }
        }
    }
}
