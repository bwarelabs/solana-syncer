package com.bwarelabs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
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
import solana.storage.ConfirmedBlock.ConfirmedBlockOuterClass.ConfirmedBlock;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.github.luben.zstd.ZstdInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

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
    private final String MACHINE_IDENTIFIER;
    private final List<String> uploadedRanges = new ArrayList<>();
    private final BigtableDataSettings settings;
    private final BigtableDataClient dataClient;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final ExecutorService executorService;

    public void startLoggingForkJoinPoolStatus(ForkJoinPool pool) {
        final Runnable loggerTask = () -> logForkJoinPoolStatus(pool);
        //scheduler.scheduleAtFixedRate(loggerTask, 0, 300, TimeUnit.SECONDS);
    }


    private final char[] CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

    public BigTableToCosWriter(Properties properties) throws IOException {
        LogManager.getLogManager().readConfiguration(
                BigTableToCosWriter.class.getClassLoader().getResourceAsStream("logging.properties"));

        this.THREAD_COUNT = Integer.parseInt(Utils.getRequiredProperty(properties, "bigtable.thread-count"));
        this.SUBRANGE_SIZE = Integer.parseInt(Utils.getRequiredProperty(properties, "bigtable.subrange-size"));
        this.BLOCKS_LAST_KEY = Utils.getRequiredProperty(properties, "bigtable.blocks-last-key");
        this.BLOCKS_START_KEY = Utils.getRequiredProperty(properties, "bigtable.blocks-start-key");
        this.ENTRIES_START_KEY = Utils.getRequiredProperty(properties, "bigtable.entries-start-key");
        this.ENTRIES_LAST_KEY = Utils.getRequiredProperty(properties, "bigtable.entries-last-key");
        this.SYNC_TYPE = Utils.getRequiredProperty(properties, "sync.type");
        this.MACHINE_IDENTIFIER = Utils.getRequiredProperty(properties, "machine-identifier");

        String projectId = Utils.getRequiredProperty(properties, "bigtable.project-id");
        String instanceId = Utils.getRequiredProperty(properties, "bigtable.instance-id");
        String pathToCredentials = Utils.getRequiredProperty(properties, "bigtable.credentials");
        // Load credentials from JSON key file
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(pathToCredentials));

        BigtableDataSettings.Builder settingsBuilder = BigtableDataSettings.newBuilder().setProjectId(projectId)
                .setInstanceId(instanceId)
                .setAppProfileId("default")
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials));

        settingsBuilder
                .stubSettings();

        settings = settingsBuilder.build();
        dataClient = BigtableDataClient.create(settings);

        executorService = Executors.newFixedThreadPool(this.THREAD_COUNT);
    }

    public void write(String tableName) throws Exception {
        logger.info("Starting BigTable to COS writer");
        loadUploadedRanges(tableName);

        for (String range: uploadedRanges) {
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

        for (Future<?> task: tasks) {
            task.get();

        }

        logger.info(String.format("Table '%s' processed and uploaded.", tableName));

        dataClient.close();
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


    private List<Future<?>> writeBlocksOrEntries(String table) throws Exception {
        logger.info(String.format("Starting BigTable to COS writer for table '%s'", table));

        String lastKey = table.equals("entries") ? this.ENTRIES_LAST_KEY : this.BLOCKS_LAST_KEY;
        String startKey = table.equals("entries") ? this.ENTRIES_START_KEY : this.BLOCKS_START_KEY;

        List<String[]> hexRanges = this
                .splitHexRange(startKey, lastKey);

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
//            tasks.add(runTaskOnWorkerThread(i, table, startRow, endRow));
        }

        return tasks;
    }

    private Future<?> runTaskOnWorkerThread(int threadId, String tableName, String startRowKey, String endRowKey) {
        return executorService.submit(() -> splitRangeAndChainUploads(threadId, tableName, startRowKey, endRowKey));
    }

    private void splitRangeAndChainUploads(int threadId, String tableName, String currentStartRow,
                                           String endRowKey) {

        if (currentStartRow.compareTo(endRowKey) >= 0) {
            return;
        }

        logger.info(String.format("Queueing task for thread %s, table %s, range %s - %s", threadId, tableName,
                currentStartRow, endRowKey));
        try {
            while (currentStartRow.compareTo(endRowKey) <= 0) {
                String currentEndRow = fetchBatch(tableName, currentStartRow, endRowKey,
                        0);
                if (currentEndRow == null) {
                    // empty batch, we're done
                    logger.info(String.format("[%s] - empty batch for %s - %s", threadId, currentStartRow, currentEndRow));
                    return;
                }

                logger.info(
                        String.format("[%s] - Processed batch %s - %s", threadId, currentStartRow, currentEndRow));
                updateUploadedRanges(threadId, currentStartRow, currentEndRow, tableName);
                currentStartRow = currentEndRow;
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, String.format("Error processing range %s - %s in table %s",
                    currentStartRow, endRowKey, tableName), e);
            e.printStackTrace();
        }
    }

    private String fetchBatch(String tableName, String startRowKey, String endRowKey, int retryCount) throws IOException {
        if (retryCount > 1) {
            return null;
        }

        if (tableName.equals("blocks") || tableName.equals("entries")) {
            BigInteger nonFormattedEndRowKey = new BigInteger(startRowKey, 16).add(BigInteger.valueOf(this.SUBRANGE_SIZE)).subtract(BigInteger.ONE);
            endRowKey = this.formatHex(nonFormattedEndRowKey);
        }

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setStrings(
                "io.serializations",
                ResultSerialization.class.getName(),
                WritableSerialization.class.getName());

        Row lastRow = null;
        try (CustomS3FSDataOutputStream outputStream = getS3OutputStream(tableName, startRowKey, endRowKey);
             CustomSequenceFileWriter customWriter = new CustomSequenceFileWriter(hadoopConfig, outputStream)) {
            logger.info(String.format("Before fetch batch for %s - %s", startRowKey, endRowKey));
            ByteStringRange range = ByteStringRange.unbounded().startClosed(startRowKey).endClosed(endRowKey);
            Query query = Query.create(TableId.of(tableName)).range(range).limit(SUBRANGE_SIZE);

            int rows = 0;
            for (Row row: dataClient.readRows(query)) {
                rows++;
                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(row.getKey().toByteArray());      
                row.getCells().forEach(cell -> {
                    assert(cell.getQualifier().toStringUtf8().equals("proto"));
                    try {
                        InputStream input = cell.getValue().newInput();
                        // highly unorthodox but there is no bincode implementation for java
                        // so we rely on the first 4 bytes being an encoding of an enum
                        byte[] decompressMethod = input.readNBytes(4);
                        ConfirmedBlock block = null;
                        if (decompressMethod[0] == 0) {
                            // no compression
                            block = ConfirmedBlock.parseFrom(input);
                        } else if (decompressMethod[0] == 1) {
                            // bzip2
                            try (BZip2CompressorInputStream decompressor = new BZip2CompressorInputStream(cell.getValue().newInput())) {
                                block = ConfirmedBlock.parseFrom(decompressor);
                            }
                        } else if (decompressMethod[0] == 2) {
                            // gzip
                            try (GzipCompressorInputStream decompressor = new GzipCompressorInputStream(cell.getValue().newInput())) {
                                block = ConfirmedBlock.parseFrom(decompressor);
                            }
                        } else {
                            // zstd
                            assert(decompressMethod[0] == 3);
                            try (ZstdInputStream decompressor = new ZstdInputStream(input)) {
                                block = ConfirmedBlock.parseFrom(decompressor);                                
                            }
                        }
                        System.out.println("got block " + block.getBlockhash());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }                    
                });         
                customWriter.append(rowKey, row);
                lastRow = row;
            }
            customWriter.close();

            logger.info(String.format("Finished after %d rows in fetch batch for %s - %s", rows, startRowKey, endRowKey));

            try {
                outputStream.getUploadFuture().join();
                logger.info(String.format("Finished upload for fetch batch for %s - %s", startRowKey, endRowKey));
            } catch (CosClientException e) {
                e.printStackTrace();
                return fetchBatch(tableName, startRowKey, endRowKey, retryCount + 1);
            }
        }

        if (lastRow != null) {
            return lastRow.getKey().toStringUtf8();
        }
        return null;
    }

    private CustomS3FSDataOutputStream getS3OutputStream(String tableName, String startRowKey,
                                                         String endRowKey) throws IOException {
        logger.info(String.format("Converting batch to sequence file format for %s from %s to %s",
                tableName, startRowKey, endRowKey));

        if (tableName.equals("tx-by-addr")) {
            startRowKey = startRowKey.replace("/", "_");
            endRowKey = endRowKey.replace("/", "_");
        }

        return new CustomS3FSDataOutputStream(
                Paths.get("output/sequencefile/" + tableName + "/range_" + startRowKey + "_" + endRowKey), tableName,
                SYNC_TYPE);
    }

    private void updateUploadedRanges(int threadId, String startRowKey, String endRowKey, String tableName) {
        uploadedRanges.add(String.format("%s_%s", startRowKey, endRowKey));
        saveUploadedRanges(threadId, startRowKey, endRowKey, tableName);
    }

    private void saveUploadedRanges(int threadId, String startRowKey, String endRowKey, String tableName) {
        try {
            synchronized (this) { // Synchronize to ensure thread-safe writing
                CosUtils.saveUploadedRangesToCos(tableName, this.MACHINE_IDENTIFIER, startRowKey, endRowKey);
            }
        } catch (Exception e) {
            logger.severe(String.format("Error saving checkpoint for thread %s - %s", threadId, e));
        }
    }

    private void loadUploadedRanges(String tableName) {
        try {
            List<String> lines = CosUtils.loadUploadedRangesFromCos(tableName);
            uploadedRanges.addAll(lines);
        } catch (IOException e) {
            logger.severe("Error loading checkpoints for table " + tableName);
            e.printStackTrace();
        }
    }

    public List<String[]> splitHexRange(String startKey, String lastKey) {
        BigInteger start = new BigInteger(startKey, 16);
        BigInteger end = new BigInteger(lastKey, 16);

        // Align start to the nearest subrange in order to get multiples of subrange size
        start = start.divide(BigInteger.valueOf(this.SUBRANGE_SIZE))
                .multiply(BigInteger.valueOf(this.SUBRANGE_SIZE));

        BigInteger totalRange = end.subtract(start).add(BigInteger.ONE);

        BigInteger numberOfSubranges = totalRange.divide(BigInteger.valueOf(this.SUBRANGE_SIZE)).add(BigInteger.ONE);

        List<String[]> intervals = new ArrayList<>();
        BigInteger currentStart = start;

        for (int i = 0; i < numberOfSubranges.intValue(); i++) {
            BigInteger currentEnd = currentStart.add(BigInteger.valueOf(this.SUBRANGE_SIZE)).subtract(BigInteger.ONE);

            intervals.add(new String[] {
                    formatHex(currentStart),
                    formatHex(currentEnd)
            });
            currentStart = currentEnd.add(BigInteger.ONE);
        }
        return intervals;
    }

    private String formatHex(BigInteger value) {
        return String.format("%016x", value);
    }
}
