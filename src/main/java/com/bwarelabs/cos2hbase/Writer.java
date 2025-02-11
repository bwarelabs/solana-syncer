package com.bwarelabs.cos2hbase;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.lang.reflect.InvocationTargetException;

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

import com.bwarelabs.common.BigtableBlock;
import com.bwarelabs.common.BigtableCell;
import com.bwarelabs.common.CloseableExecutorService;
import com.bwarelabs.common.CosDownloader;
import com.bwarelabs.common.CosUtils;
import com.bwarelabs.common.Utils;

public class Writer {

    private static final Logger logger = Logger.getLogger(Writer.class.getName());

    private final byte[] COLUMN_FAMILY = Bytes.toBytes("x");
    private final byte[] BIN_QUALIFIER = Bytes.toBytes("bin");
    private final byte[] PROTO_QUALIFIER = Bytes.toBytes("proto");

    private final int START_KEY;
    private final int END_KEY;
    private final String WORKSPACE;
    private final int CONFIG_POOL_SIZE;

    private final Configuration CONFIG_HBASE;
    private final Configuration CONFIG_HADOOP;

    private final CosUtils cosUtils;

    public Writer(CosUtils cosUtils, Properties properties, int startKey, int endKey) throws Exception {
        START_KEY = startKey;
        END_KEY = endKey;

        WORKSPACE = Utils.getRequiredProperty(properties, "hbase.download-dir");

        // Hbase configuration
        CONFIG_HBASE = HBaseConfiguration.create();
        CONFIG_HBASE.set("hbase.zookeeper.quorum", Utils.getRequiredProperty(properties, "hbase.zookeeper.quorum"));
        CONFIG_HBASE.set("hbase.zookeeper.property.clientPort",
                Utils.getRequiredProperty(properties, "hbase.zookeeper.property.clientPort"));
        CONFIG_HBASE.set("hbase.client.keyvalue.maxsize",
                Utils.getRequiredProperty(properties, "hbase.client.keyvalue.maxsize"));

        // Hadoop configuration
        CONFIG_HADOOP = new Configuration();
        CONFIG_HADOOP.setStrings("io.serializations", ResultSerialization.class.getName(),
                WritableSerialization.class.getName());

        CONFIG_POOL_SIZE = Utils.getRequiredIntegerProperty(properties, "hbase.worker-pool.size");

        this.cosUtils = cosUtils;
    }

    public void write() throws Exception {
        String regex = "range_([0-9a-fA-F]+)_([0-9a-fA-F]+)";
        Pattern pattern = Pattern.compile(regex);

        int end_boundary = END_KEY;
        int start_boundary = START_KEY;

        try (Connection connection = ConnectionFactory.createConnection(CONFIG_HBASE);
                CloseableExecutorService executor = createExecutor(CONFIG_POOL_SIZE);
                CosDownloader cosDownloader = cosUtils.createCosDownloader()) {
            // Create Hbase tables if they don't exist
            createTables(connection);

            try (Table snapshots = connection.getTable(TableName.valueOf("snapshots"))) {
                cosUtils.walkKeysByPrefix((key) -> {
                    Matcher matcher = pattern.matcher(key);
                    if (!matcher.find()) {
                        return;
                    }
                    String start = matcher.group(1);
		            String end = matcher.group(2);
                    int start_value = Integer.parseInt(start, 16);
		            int end_value = Integer.parseInt(end, 16);
                    if (Math.max(start_value, start_boundary) >= Math.min(end_value, end_boundary)) {
                        return;
                    }

                    String dstPath = WORKSPACE + "/" + key;
                    try {
                        byte[] cosKey = key.getBytes();

                        logger.info("Checking block range completion: " + key);
                        Get get = new Get(cosKey);
                        Result result = snapshots.get(get);
                        if (!result.isEmpty()) {
                            logger.info("Block range already processed: " + key);
                            return;
                        }

                        boolean isBlock = key.contains("blocks");
                        boolean isEntries = key.contains("entries");
                        if (!isBlock && !isEntries) {
                            logger.info("Skipping key: " + key);
                            return;
                        }

                        logger.info("Downloading data in range: " + key);
                        cosDownloader.downloadByKey(key, dstPath);

                        logger.info("Processing data in range: " + key);
                        if (isBlock) {
                            processBlock(connection, executor.getExecutor(), dstPath);
                        }
                        if (isEntries) {
                            processEntries(connection, executor.getExecutor(), dstPath);
                        }

                        // Insert marker for range completion so we don't reprocess it
                        Put put_status = new Put(cosKey);
                        put_status.addColumn(COLUMN_FAMILY, "status".getBytes(), new byte[] {});
                        snapshots.put(put_status);

                    } catch (Exception e) {
                        logger.severe("Error processing key: " + e.getMessage());
                        throw new RuntimeException(e);
                    } finally {
                        File file = new File(dstPath);
                        if (file.exists()) {
                            file.delete();
                        }
                    }
                });
            }
        }
        logger.info("Finished processing all keys");
    }

    public static CloseableExecutorService createExecutor(int poolSize) {
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(2 * poolSize);
        var executor = new ThreadPoolExecutor(
                poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, workQueue,
                new ThreadPoolExecutor.CallerRunsPolicy());
        return new CloseableExecutorService(executor);
    }

    private void createTables(Connection conn) throws IOException {
        var requiredTables = new String[] { "blocks", "entries", "tx", "tx-by-addr", "snapshots" };

        var columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build();
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

    private void processBlock(Connection conn, ExecutorService executor, String dstPath)
            throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, InterruptedException,
            ExecutionException {
        Path blockFile = new Path(dstPath);
        try (SequenceFile.Reader reader = new SequenceFile.Reader(CONFIG_HADOOP,
                SequenceFile.Reader.file(blockFile))) {
            ImmutableBytesWritable key = (ImmutableBytesWritable) reader.getKeyClass().getDeclaredConstructor()
                    .newInstance();

            List<Future<?>> futures = new ArrayList<>();

            Result result = null;
            while (reader.next(key)) {
                result = (Result) reader.getCurrentValue(result);

                var resultQualifier = result.rawCells()[0].getQualifierArray().clone();
                var resultKey = key.get().clone();
                var resultValue = result.value().clone();

                var ft = executor.submit(() -> {
                    try (BufferedMutator tx = conn.getBufferedMutator(TableName.valueOf("tx"));
                            BufferedMutator txByAddr = conn.getBufferedMutator(TableName.valueOf("tx-by-addr"));
                            Table blocks = conn.getTable(TableName.valueOf("blocks"))) {

                        Put put = new Put(resultKey);
                        put.addColumn(COLUMN_FAMILY, resultQualifier, resultValue);
                        blocks.put(put);

                        String rowKey = new String(resultKey, 0, resultKey.length);
                        BigtableBlock block = new BigtableBlock(rowKey,
                                resultQualifier,
                                resultValue);
                        for (BigtableCell cell : block.txs) {
                            Put txPut = new Put(cell.key().getBytes());
                            txPut.addColumn(COLUMN_FAMILY, BIN_QUALIFIER, cell.value());
                            tx.mutate(txPut);
                        }
                        for (BigtableCell cell : block.txByAddr) {
                            Put txByAddrPut = new Put(cell.key().getBytes());
                            txByAddrPut.addColumn(COLUMN_FAMILY, PROTO_QUALIFIER, cell.value());
                            txByAddr.mutate(txByAddrPut);
                        }
                    } catch (IOException | IllegalArgumentException | SecurityException e) {
                        logger.severe("Error processing key: " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
                futures.add(ft);
            }
            for (Future<?> future : futures) {
                // Wait for all futures to complete
                future.get();
            }
        }
    }

    private void processEntries(Connection conn, ExecutorService executor, String dstPath)
            throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, InterruptedException,
            ExecutionException {
        Path entriesFile = new Path(dstPath);
        try (SequenceFile.Reader reader = new SequenceFile.Reader(CONFIG_HADOOP,
                SequenceFile.Reader.file(entriesFile))) {
            ImmutableBytesWritable key = (ImmutableBytesWritable) reader.getKeyClass().getDeclaredConstructor()
                    .newInstance();

            List<Future<?>> futures = new ArrayList<>();

            Result result = null;
            while (reader.next(key)) {
                result = (Result) reader.getCurrentValue(result);

                var resultQualifier = result.rawCells()[0].getQualifierArray().clone();
                var resultKey = key.get().clone();
                var resultValue = result.value().clone();

                var ft = executor.submit(() -> {
                    try (Table entries = conn.getTable(TableName.valueOf("entries"))) {
                        Put put = new Put(resultKey);
                        put.addColumn(COLUMN_FAMILY, resultQualifier, resultValue);
                        entries.put(put);
                    } catch (IOException | IllegalArgumentException | SecurityException e) {
                        logger.severe("Error processing key: " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
                futures.add(ft);
            }
            for (Future<?> future : futures) {
                // Wait for all futures to complete
                future.get();
            }
        }
    }
}
