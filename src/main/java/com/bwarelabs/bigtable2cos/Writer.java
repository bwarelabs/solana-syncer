package com.bwarelabs.bigtable2cos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;

import com.bwarelabs.common.CosUtils;
import com.bwarelabs.common.CustomS3FSDataOutputStream;
import com.bwarelabs.common.CustomSequenceFileWriter;
import com.bwarelabs.common.Utils;
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
import java.util.logging.Logger;
import java.util.logging.Level;

public class Writer implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(Writer.class.getName());

    private final int SUBRANGE_SIZE;
    private final String START_KEY;
    private final String END_KEY;
    private final String SYNC_TYPE;
    private final List<String> uploadedRanges = new ArrayList<>();
    private final BigtableDataClient dataClient;
    private final ExecutorService executorService;
    private final String TABLE_NAME;

    private final CosUtils cosUtils;

    public Writer(CosUtils cosUtils, Properties properties, String startKey, String endKey, boolean useEmulator)
            throws Exception {
        this.cosUtils = cosUtils;

        START_KEY = startKey;
        END_KEY = endKey;

        int THREAD_COUNT = Utils.getRequiredIntegerProperty(properties, "bigtable.thread-count");
        SUBRANGE_SIZE = Utils.getRequiredIntegerProperty(properties, "bigtable.subrange-size");

        TABLE_NAME = Utils.getRequiredProperty(properties, "bigtable.table-name");
        if (!TABLE_NAME.equals("blocks") && !TABLE_NAME.equals("entries")) {
            logger.severe("Invalid table name. Only 'blocks' and 'entries' are supported.");
            throw new Exception("Invalid table name. Only 'blocks' and 'entries' are supported.");
        }

        SYNC_TYPE = Utils.getRequiredProperty(properties, "sync.type");
        String projectId = Utils.getRequiredProperty(properties, "bigtable.project-id");
        String instanceId = Utils.getRequiredProperty(properties, "bigtable.instance-id");

        BigtableDataSettings.Builder settingsBuilder = BigtableDataSettings.newBuilder().setProjectId(projectId)
                .setInstanceId(instanceId).setAppProfileId("default")
                .setMetricsProvider(NoopMetricsProvider.INSTANCE);

        if (!useEmulator) {
            String pathToCredentials = Utils.getRequiredProperty(properties, "bigtable.credentials");
            // Load credentials from JSON key file
            GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(pathToCredentials));
            settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }
        settingsBuilder.stubSettings().setMetricsProvider(NoopMetricsProvider.INSTANCE);

        dataClient = BigtableDataClient.create(settingsBuilder.build());
        executorService = Executors.newFixedThreadPool(THREAD_COUNT);
    }

    public void write() throws Exception {
        loadUploadedRanges(TABLE_NAME);
        for (String range : uploadedRanges) {
            logger.info("Already uploaded range: " + range);
        }

        List<Future<?>> tasks = writeBlocks();

        for (Future<?> task : tasks) {
            task.get();
        }

        logger.info(String.format("Table '%s' processed and uploaded.", TABLE_NAME));
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

            logger.info(String.format("Table: %s, Range: %s - %s", TABLE_NAME, startRow, endRow));
            tasks.add(runTaskOnWorkerThread(startRow, endRow));
        }

        return tasks;
    }

    private Future<?> runTaskOnWorkerThread(String startRowKey, String endRowKey) {
        return executorService.submit(() -> startFetchBatch(startRowKey, endRowKey));
    }

    private void startFetchBatch(String startRowKey, String endRowKey) {
        long threadId = Thread.currentThread().getId();
        logger.info(String.format("Thread %s starting task for table %s, range %s - %s", threadId, TABLE_NAME,
                startRowKey, endRowKey));

        try {
            fetchBatch(startRowKey, endRowKey, 0);

            logger.info(String.format("[%s] - Processed batch %s - %s", threadId, startRowKey, endRowKey));
            updateUploadedRanges(startRowKey, endRowKey);
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                    String.format("Error processing range %s - %s in table %s", startRowKey, endRowKey,
                            TABLE_NAME),
                    e);
            e.printStackTrace();
            cosUtils.saveFailedRangesToCos(TABLE_NAME, startRowKey, endRowKey);
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

        try (CustomS3FSDataOutputStream outputStream = getS3OutputStream(TABLE_NAME, startRowKey, endRowKey);
                CustomSequenceFileWriter customWriter = new CustomSequenceFileWriter(hadoopConfig,
                        outputStream)) {
            logger.info(String.format("Before fetch batch for %s - %s", startRowKey, endRowKey));

            ByteStringRange range = ByteStringRange.unbounded().startClosed(startRowKey).endClosed(endRowKey);
            Query query = Query.create(TableId.of(TABLE_NAME)).range(range).limit(SUBRANGE_SIZE);

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
        return new CustomS3FSDataOutputStream(cosUtils, range, tableName, SYNC_TYPE);
    }

    private void updateUploadedRanges(String startRowKey, String endRowKey) {
        uploadedRanges.add(String.format("%s_%s", startRowKey, endRowKey));
        saveUploadedRanges(startRowKey, endRowKey);
    }

    private void saveUploadedRanges(String startRowKey, String endRowKey) {
        try {
            cosUtils.saveUploadedRangesToCos(TABLE_NAME, startRowKey, endRowKey);
        } catch (Exception e) {
            logger.severe(String.format("Error saving checkpoint for table %s with range %s - %s", TABLE_NAME,
                    startRowKey, endRowKey));
            e.printStackTrace();
            throw e;
        }
    }

    private void loadUploadedRanges(String tableName) {
        try {
            List<String> lines = cosUtils.loadUploadedRangesFromCos(tableName);
            uploadedRanges.addAll(lines);
        } catch (Exception e) {
            logger.severe("Error loading checkpoints for table " + tableName);
            e.printStackTrace();
        }
    }

    public List<String[]> splitHexRange() {
        BigInteger start = new BigInteger(START_KEY, 10);
        BigInteger end = new BigInteger(END_KEY, 10);

        // Align start down to the nearest subrange in order to get multiples of
        // subrange size
        start = start.divide(BigInteger.valueOf(SUBRANGE_SIZE)).multiply(BigInteger.valueOf(SUBRANGE_SIZE));

        BigInteger totalRange = end.subtract(start).add(BigInteger.ONE);
        BigInteger numberOfSubranges = totalRange.divide(BigInteger.valueOf(SUBRANGE_SIZE));

        if (totalRange.mod(BigInteger.valueOf(SUBRANGE_SIZE)).compareTo(BigInteger.ZERO) > 0) {
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

    @Override
    public void close() throws Exception {
        dataClient.close();
        executorService.shutdown();
    }
}
