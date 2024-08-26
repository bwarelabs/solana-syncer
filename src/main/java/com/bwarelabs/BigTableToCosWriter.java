package com.bwarelabs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.slf4j.LoggerFactory;
import com.qcloud.cos.exception.*;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.stub.metrics.NoopMetricsProvider;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;

public class BigTableToCosWriter {
    private static final Logger logger = Logger.getLogger(BigTableToCosWriter.class.getName());
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BigTableToCosWriter.class);

    private final int THREAD_COUNT;
    private final int SUBRANGE_SIZE;
    private final String BLOCKS_LAST_KEY;
    private final String BLOCKS_START_KEY;
    private final String ENTRIES_START_KEY;
    private final String ENTRIES_LAST_KEY;
    private final String SYNC_TYPE;
    private final List<String> uploadedRanges = new ArrayList<>();
    private final BigtableDataClient dataClient;
    private final ExecutorService executorService;

    public BigTableToCosWriter(Properties properties, String blocksStartKey, String blocksLastKey) throws Exception {
        LogManager.getLogManager().readConfiguration(
                BigTableToCosWriter.class.getClassLoader().getResourceAsStream("logging.properties"));

        this.THREAD_COUNT = Integer.parseInt(Utils.getRequiredProperty(properties, "bigtable.thread-count"));
        this.SUBRANGE_SIZE = Integer.parseInt(Utils.getRequiredProperty(properties, "bigtable.subrange-size"));
        this.BLOCKS_START_KEY = blocksStartKey != null ? blocksStartKey : Utils.getRequiredProperty(properties, "bigtable.blocks-start-key");
        this.BLOCKS_LAST_KEY =  blocksLastKey != null ? blocksLastKey : Utils.getRequiredProperty(properties, "bigtable.blocks-last-key");
        this.ENTRIES_START_KEY = Utils.getRequiredProperty(properties, "bigtable.entries-start-key");
        this.ENTRIES_LAST_KEY = Utils.getRequiredProperty(properties, "bigtable.entries-last-key");
        this.SYNC_TYPE = Utils.getRequiredProperty(properties, "sync.type");

        String projectId = Utils.getRequiredProperty(properties, "bigtable.project-id");
        String instanceId = Utils.getRequiredProperty(properties, "bigtable.instance-id");
        String pathToCredentials = Utils.getRequiredProperty(properties, "bigtable.credentials");
        // Load credentials from JSON key file
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(pathToCredentials));

        BigtableDataSettings.Builder settingsBuilder = BigtableDataSettings.newBuilder().setProjectId(projectId)
                .setInstanceId(instanceId).setAppProfileId("default")
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .setMetricsProvider(NoopMetricsProvider.INSTANCE);

        settingsBuilder.stubSettings().setMetricsProvider(NoopMetricsProvider.INSTANCE);

        dataClient = BigtableDataClient.create(settingsBuilder.build());

        executorService = Executors.newFixedThreadPool(this.THREAD_COUNT);
    }

    public void write(String tableName) throws Exception {
        logger.info("Starting BigTable to COS writer");
        loadUploadedRanges(tableName);

        for (String range : uploadedRanges) {
            logger.info("Already uploaded range: " + range);
        }

        if (tableName == null || tableName.trim().isEmpty()) {
            logger.severe("Table name cannot be null or empty");
            return;
        }

        List<Future<?>> tasks;
        if (tableName.equals("blocks") || tableName.equals("entries")) {
            tasks = writeBlocksOrEntries(tableName);
        } else {
            logger.severe("Invalid table name: " + tableName);
            tasks = new ArrayList<>();
        }

        for (Future<?> task : tasks) {
            task.get();

        }

        logger.info(String.format("Table '%s' processed and uploaded.", tableName));

        dataClient.close();
        executorService.shutdown();
        logger.info("BigTable to COS writer completed");
    }

    private void logForkJoinPoolStatus(ForkJoinPool pool) {
        logger.info("ForkJoinPool status:");
        logger.info("Parallelism: " + pool.getParallelism());
        logger.info("Pool size: " + pool.getPoolSize());
        logger.info("Active thread count: " + pool.getActiveThreadCount());
        logger.info("Running thread count: " + pool.getRunningThreadCount());
        logger.info("Queued task count: " + pool.getQueuedTaskCount());
        logger.info("Queued submission count: " + pool.getQueuedSubmissionCount());
        logger.info("Steal count: " + pool.getStealCount());
        logger.info("Is pool quiescent: " + pool.isQuiescent());
        logger.info("--------------------------------");
    }

    private List<Future<?>> writeBlocksOrEntries(String table) {
        logger.info(String.format("Starting BigTable to COS writer for table '%s'", table));

        String lastKey = table.equals("entries") ? this.ENTRIES_LAST_KEY : this.BLOCKS_LAST_KEY;
        String startKey = table.equals("entries") ? this.ENTRIES_START_KEY : this.BLOCKS_START_KEY;

        List<String[]> hexRanges = this.splitHexRange(startKey, lastKey);

        logger.info("Thread count: " + this.THREAD_COUNT);

        List<Future<?>> tasks = new ArrayList<>();

        for (String[] hexRange : hexRanges) {
            String startRow = hexRange[0];
            String endRow = hexRange[1];

            if (uploadedRanges.contains(startRow + "_" + endRow)) {
                logger.info(String.format("Range %s - %s already uploaded, skipping", startRow, endRow));
                continue;
            }

            logger.info(String.format("Table: %s, Range: %s - %s", table, startRow, endRow));
            tasks.add(runTaskOnWorkerThread(table, startRow, endRow));
        }

        return tasks;
    }

    private Future<?> runTaskOnWorkerThread(String tableName, String startRowKey, String endRowKey) {
        return executorService.submit(() -> startFetchBatch(tableName, startRowKey, endRowKey));
    }

    private void startFetchBatch(String tableName, String startRowKey, String endRowKey) {
        long threadId = Thread.currentThread().getId();
        logger.info(String.format("Thread %s starting task for table %s, range %s - %s", threadId, tableName,
                startRowKey, endRowKey));

        try {
            fetchBatch(tableName, startRowKey, endRowKey, 0);

            logger.info(String.format("[%s] - Processed batch %s - %s", threadId, startRowKey, endRowKey));
            updateUploadedRanges(startRowKey, endRowKey, tableName);
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                    String.format("Error processing range %s - %s in table %s", startRowKey, endRowKey, tableName),
                    e);
            e.printStackTrace();
            CosUtils.saveFailedRangesToCos(tableName, startRowKey, endRowKey);
        }
    }

    private static CellBuilder createCellBuilder(String family, String qualifier, long timestamp) {
        CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
        return cellBuilder
                .setFamily(Bytes.toBytes(family))
                .setQualifier(Bytes.toBytes(qualifier))
                .setTimestamp(TimestampConverter.bigtable2hbase(timestamp))
                .setType(Cell.Type.Put);
    }

    private void fetchBatch(String tableName, String startRowKey, String endRowKey, int retryCount)
            throws IOException {
        if (retryCount > 1) {
            throw new IOException("Failed to fetch batch after 2 retries");
        }

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setStrings("io.serializations", ResultSerialization.class.getName(),
                WritableSerialization.class.getName());

        try (CustomS3FSDataOutputStream blocksOutputStream = getS3OutputStream(tableName, startRowKey, endRowKey);
                CustomSequenceFileWriter blocksCustomWriter = new CustomSequenceFileWriter(hadoopConfig,
                        blocksOutputStream)) {
            logger.info(String.format("Before fetch batch for %s - %s", startRowKey, endRowKey));
            ByteStringRange range = ByteStringRange.unbounded().startClosed(startRowKey).endClosed(endRowKey);
            Query query = Query.create(TableId.of(tableName)).range(range).limit(SUBRANGE_SIZE);

            int rows = 0;

            try {
                for (Row row : dataClient.readRows(query)) {
                    rows++;
                    ImmutableBytesWritable rowKey = new ImmutableBytesWritable(row.getKey().toByteArray());
                    blocksCustomWriter.append(rowKey, row);
                }
            } catch (Exception e) {
                if (blocksOutputStream.isControlledClose()) {
                    logger.info("Controlled close exception");
                    logger.info("blocksOutputStream.isControlledClose(): " + blocksOutputStream.isControlledClose());
                }

                e.printStackTrace();
                throw e;
            }

            blocksCustomWriter.close();

            logger.info(
                    String.format("Finished after %d rows in fetch batch for %s - %s", rows, startRowKey, endRowKey));

            blocksOutputStream.getUploadFuture().join();
            logger.info(String.format("Finished upload for fetch batch for %s - %s", startRowKey, endRowKey));
        } catch (Exception e) {
            e.printStackTrace();
            logger.severe(String.format("Error fetching batch for %s - %s. Retrying...", startRowKey, endRowKey));
            fetchBatch(tableName, startRowKey, endRowKey, retryCount + 1);
        }
    }

    private void fetchAndWriteData(CustomSequenceFileWriter writer, TableId table, Set<String> keys) {

        try {
            logger.info("fetching data for " + keys.size() + " keys");
            int nKeys = 0;
            Query query = Query.create(table);
            for (String key : keys) {
                if (nKeys < 1000) {
                    query = query.rowKey(key);
                    nKeys++;
                    continue;
                }

                for (Row row : dataClient.readRows(query)) {
                    ImmutableBytesWritable rowKey = new ImmutableBytesWritable(row.getKey().toByteArray());
                    writer.append(rowKey, row);
                }
                nKeys = 0;
                query = Query.create(table);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private CustomS3FSDataOutputStream getS3OutputStream(String tableName, String startRowKey, String endRowKey)
            throws IOException {
        logger.info(String.format("Converting batch to sequence file format for %s from %s to %s", tableName,
                startRowKey, endRowKey));

        if (tableName.equals("tx-by-addr")) {
            startRowKey = startRowKey.replace("/", "_");
            endRowKey = endRowKey.replace("/", "_");
        }

        return new CustomS3FSDataOutputStream(
                Paths.get("output/sequencefile/" + tableName + "/range_" + startRowKey + "_" + endRowKey), tableName,
                SYNC_TYPE);
    }

    private void updateUploadedRanges(String startRowKey, String endRowKey, String tableName) {
        uploadedRanges.add(String.format("%s_%s", startRowKey, endRowKey));
        saveUploadedRanges(startRowKey, endRowKey, tableName);
    }

    private void saveUploadedRanges(String startRowKey, String endRowKey, String tableName) {
        try {
            CosUtils.saveUploadedRangesToCos(tableName, startRowKey, endRowKey);
        } catch (Exception e) {
            logger.severe(String.format("Error saving checkpoint for table %s with range %s - %s", tableName,
                    startRowKey, endRowKey));
            e.printStackTrace();
            throw e;
        }
    }

    private void loadUploadedRanges(String tableName) {
        try {
            List<String> lines = CosUtils.loadUploadedRangesFromCos(tableName);
            uploadedRanges.addAll(lines);
        } catch (Exception e) {
            logger.severe("Error loading checkpoints for table " + tableName);
            e.printStackTrace();
        }
    }

    public List<String[]> splitHexRange(String startKey, String lastKey) {
        BigInteger start = new BigInteger(startKey, 16);
        BigInteger end = new BigInteger(lastKey, 16);

        // Align start down to the nearest subrange in order to get multiples of
        // subrange size
        start = start.divide(BigInteger.valueOf(this.SUBRANGE_SIZE)).multiply(BigInteger.valueOf(this.SUBRANGE_SIZE));

        BigInteger totalRange = end.subtract(start).add(BigInteger.ONE);
        BigInteger numberOfSubranges = totalRange.divide(BigInteger.valueOf(this.SUBRANGE_SIZE));

        if (totalRange.mod(BigInteger.valueOf(this.SUBRANGE_SIZE)).compareTo(BigInteger.ZERO) > 0) {
            numberOfSubranges = numberOfSubranges.add(BigInteger.ONE);
        }

        List<String[]> intervals = new ArrayList<>();
        BigInteger currentStart = start;

        for (int i = 0; i < numberOfSubranges.intValue(); i++) {
            BigInteger currentEnd = currentStart.add(BigInteger.valueOf(this.SUBRANGE_SIZE)).subtract(BigInteger.ONE);

            intervals.add(new String[] { formatHex(currentStart), formatHex(currentEnd) });
            currentStart = currentEnd.add(BigInteger.ONE);
        }
        return intervals;
    }

    private String formatHex(BigInteger value) {
        return String.format("%016x", value);
    }
}
