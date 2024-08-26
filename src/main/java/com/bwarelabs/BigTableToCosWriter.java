package com.bwarelabs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;

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
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.Level;

public class BigTableToCosWriter {
    private static final Logger logger = Logger.getLogger(BigTableToCosWriter.class.getName());

    private final int SUBRANGE_SIZE;
    private final String START_KEY;
    private final String END_KEY;
    private final String SYNC_TYPE;
    private final List<String> uploadedRanges = new ArrayList<>();
    private final BigtableDataClient dataClient;
    private final ExecutorService executorService;
    private final String TABLE_NAME;

    public BigTableToCosWriter(Properties properties, String startKey, String endKey) throws Exception {
        LogManager.getLogManager().readConfiguration(
                BigTableToCosWriter.class.getClassLoader().getResourceAsStream("logging.properties"));

        int THREAD_COUNT = Integer.parseInt(Utils.getRequiredProperty(properties, "bigtable.thread-count"));
        this.SUBRANGE_SIZE = Integer.parseInt(Utils.getRequiredProperty(properties, "bigtable.subrange-size"));

        this.TABLE_NAME = Utils.getRequiredProperty(properties, "bigtable.table-name");
        this.START_KEY = startKey != null ? startKey : Utils.getRequiredProperty(properties, "bigtable.start-key");
        this.END_KEY =  endKey != null ? endKey : Utils.getRequiredProperty(properties, "bigtable.end-key");

        if (!this.TABLE_NAME.equals("blocks") && !this.TABLE_NAME.equals("entries")) {
            logger.severe("Invalid table name. Only 'blocks' and 'entries' are supported.");
            throw new Exception("Invalid table name. Only 'blocks' and 'entries' are supported.");
        }

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

        executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        logger.info("Thread count: " + THREAD_COUNT);
    }

    public void write() throws Exception {
        logger.info("Starting BigTable to COS writer");
        loadUploadedRanges(this.TABLE_NAME);

        for (String range : uploadedRanges) {
            logger.info("Already uploaded range: " + range);
        }

        List<Future<?>> tasks = writeBlocks();

        for (Future<?> task : tasks) {
            task.get();
        }

        logger.info(String.format("Table '%s' processed and uploaded.", this.TABLE_NAME));

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

    private List<Future<?>> writeBlocks() {
        List<String[]> hexRanges = this.splitHexRange();
        List<Future<?>> tasks = new ArrayList<>();

        for (String[] hexRange : hexRanges) {
            String startRow = hexRange[0];
            String endRow = hexRange[1];

            if (uploadedRanges.contains(startRow + "_" + endRow)) {
                logger.info(String.format("Range %s - %s already uploaded, skipping", startRow, endRow));
                continue;
            }

            logger.info(String.format("Table: %s, Range: %s - %s", this.TABLE_NAME, startRow, endRow));
            tasks.add(runTaskOnWorkerThread(startRow, endRow));
        }

        return tasks;
    }

    private Future<?> runTaskOnWorkerThread(String startRowKey, String endRowKey) {
        return executorService.submit(() -> startFetchBatch(startRowKey, endRowKey));
    }

    private void startFetchBatch(String startRowKey, String endRowKey) {
        long threadId = Thread.currentThread().getId();
        logger.info(String.format("Thread %s starting task for table %s, range %s - %s", threadId, this.TABLE_NAME,
                startRowKey, endRowKey));

        try {
            fetchBatch(startRowKey, endRowKey, 0);

            logger.info(String.format("[%s] - Processed batch %s - %s", threadId, startRowKey, endRowKey));
            updateUploadedRanges(startRowKey, endRowKey);
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                    String.format("Error processing range %s - %s in table %s", startRowKey, endRowKey, this.TABLE_NAME),
                    e);
            e.printStackTrace();
            CosUtils.saveFailedRangesToCos(this.TABLE_NAME, startRowKey, endRowKey);
        }
    }

    private void fetchBatch(String startRowKey, String endRowKey, int retryCount)
            throws IOException {
        if (retryCount > 1) {
            throw new IOException("Failed to fetch batch after 2 retries");
        }

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setStrings("io.serializations", ResultSerialization.class.getName(),
                WritableSerialization.class.getName());

        try (CustomS3FSDataOutputStream outputStream = getS3OutputStream(this.TABLE_NAME, startRowKey, endRowKey);
                CustomSequenceFileWriter customWriter = new CustomSequenceFileWriter(hadoopConfig,
                        outputStream)) {
            logger.info(String.format("Before fetch batch for %s - %s", startRowKey, endRowKey));

            ByteStringRange range = ByteStringRange.unbounded().startClosed(startRowKey).endClosed(endRowKey);
            Query query = Query.create(TableId.of(this.TABLE_NAME)).range(range).limit(SUBRANGE_SIZE);

            int rows = 0;

            try {
                for (Row row : dataClient.readRows(query)) {
                    rows++;
                    ImmutableBytesWritable rowKey = new ImmutableBytesWritable(row.getKey().toByteArray());
                    customWriter.append(rowKey, row);
                }
            } catch (Exception e) {
                if (outputStream.isControlledClose()) {
                    logger.info("Controlled close exception");
                    logger.info("outputStream.isControlledClose(): " + outputStream.isControlledClose());
                }

                e.printStackTrace();
                throw e;
            }

            customWriter.close();

            logger.info(
                    String.format("Finished after %d rows in fetch batch for %s - %s", rows, startRowKey, endRowKey));

            outputStream.getUploadFuture().join();
            logger.info(String.format("Finished upload for fetch batch for %s - %s", startRowKey, endRowKey));
        } catch (Exception e) {
            e.printStackTrace();
            logger.severe(String.format("Error fetching batch for %s - %s. Retrying...", startRowKey, endRowKey));

            fetchBatch(startRowKey, endRowKey, retryCount + 1);
        }
    }

    private CustomS3FSDataOutputStream getS3OutputStream(String tableName, String startRowKey, String endRowKey)
            throws IOException {
        logger.info(String.format("Converting batch to sequence file format for %s from %s to %s", tableName,
                startRowKey, endRowKey));

        String range = "range_" + startRowKey + "_" + endRowKey;
        return new CustomS3FSDataOutputStream(range, tableName, SYNC_TYPE);
    }

    private void updateUploadedRanges(String startRowKey, String endRowKey) {
        uploadedRanges.add(String.format("%s_%s", startRowKey, endRowKey));
        saveUploadedRanges(startRowKey, endRowKey);
    }

    private void saveUploadedRanges(String startRowKey, String endRowKey) {
        try {
            CosUtils.saveUploadedRangesToCos(this.TABLE_NAME, startRowKey, endRowKey);
        } catch (Exception e) {
            logger.severe(String.format("Error saving checkpoint for table %s with range %s - %s", this.TABLE_NAME,
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

    public List<String[]> splitHexRange() {
        BigInteger start = new BigInteger(this.START_KEY, 16);
        BigInteger end = new BigInteger(this.END_KEY, 16);

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
