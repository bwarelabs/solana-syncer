package com.bwarelabs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.slf4j.LoggerFactory;
import com.qcloud.cos.exception.*;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.Level;

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

public class BigTableToCosWriter {
    private static final Logger logger = Logger.getLogger(BigTableToCosWriter.class.getName());
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BigTableToCosWriter.class);

    private final int THREAD_COUNT;
    private final int SUBRANGE_SIZE;
    private final int BATCH_LIMIT;
    private final String TX_LAST_KEY;
    private final String TX_BY_ADDR_LAST_KEY;
    private final String BLOCKS_LAST_KEY;
    private final String BLOCKS_START_KEY;
    private final String ENTRIES_START_KEY;
    private final String ENTRIES_LAST_KEY;
    private final String SYNC_TYPE;
    private final Map<Integer, String> checkpoints = new HashMap<>();
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
        this.BATCH_LIMIT = Integer.parseInt(Utils.getRequiredProperty(properties, "bigtable.batch-limit"));
        this.TX_LAST_KEY = Utils.getRequiredProperty(properties, "bigtable.tx-last-key");
        this.TX_BY_ADDR_LAST_KEY = Utils.getRequiredProperty(properties, "bigtable.tx-by-addr-last-key");
        this.BLOCKS_LAST_KEY = Utils.getRequiredProperty(properties, "bigtable.blocks-last-key");
        this.BLOCKS_START_KEY = Utils.getRequiredProperty(properties, "bigtable.blocks-start-key");
        this.ENTRIES_START_KEY = Utils.getRequiredProperty(properties, "bigtable.entries-start-key");
        this.ENTRIES_LAST_KEY = Utils.getRequiredProperty(properties, "bigtable.entries-last-key");
        this.SYNC_TYPE = Utils.getRequiredProperty(properties, "sync.type");

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
        loadCheckpoints(tableName);

        for (Map.Entry<Integer, String> entry : checkpoints.entrySet()) {
            logger.info("Thread: " + entry.getKey() + " Checkpoint: " + entry.getValue());
        }

        if (tableName == null || tableName.trim().isEmpty()) {
            logger.severe("Table name cannot be null or empty");
            return;
        }

        // ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        // ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor)
        // this.executorService;
        //
        // Runnable printStats = () -> {
        // System.out.println("======== Executor State ========");
        // System.out.println("Pool Size: " + threadPoolExecutor.getPoolSize());
        // System.out.println("Active Threads: " + threadPoolExecutor.getActiveCount());
        // System.out.println("Completed Tasks: " +
        // threadPoolExecutor.getCompletedTaskCount());
        // System.out.println("Total Tasks: " + threadPoolExecutor.getTaskCount());
        // System.out.println("Tasks in Queue: " +
        // threadPoolExecutor.getQueue().size());
        // System.out.println("================================");
        // };
        //
        // scheduler.scheduleAtFixedRate(printStats, 0, 1, TimeUnit.SECONDS);

        List<Future<?>> tasks;
        if (tableName.equals("blocks") || tableName.equals("entries")) {
            tasks = writeBlocksOrEntries(tableName);
        } else if (tableName.equals("tx") || tableName.equals("tx-by-addr")) {
            tasks = writeTx(tableName);
        } else {
            logger.severe("Invalid table name: " + tableName);
            tasks = new ArrayList<Future<?>>();
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
        if (hexRanges.size() != this.THREAD_COUNT) {
            throw new Exception("Invalid number of thread ranges, size must be equal to THREAD_COUNT");
        }

        logger.info("Thread count: " + this.THREAD_COUNT);

        List<Future<?>> tasks = new ArrayList<>();

        for (int i = 0; i < this.THREAD_COUNT; i++) {
            String[] hexRange = hexRanges.get(i);
            String startRow = checkpoints.getOrDefault(i, hexRange[0]);
            String endRow = hexRange[1];

            boolean isCheckpointStart = checkpoints.get(i) != null;
            logger.info("isCheckpointStart: " + isCheckpointStart);

            logger.info(String.format("Table: %s, Range: %s - %s", table, startRow, endRow));
            tasks.add(runTaskOnWorkerThread(i, table, startRow, endRow, isCheckpointStart));
        }    

        return tasks;
    }

    private List<Future<?>> writeTx(String table) throws Exception {
        logger.info(String.format("Starting BigTable to COS writer for table '%s'", table));

        List<String[]> txRanges = this.splitRangeTx();
        if (txRanges.size() != this.THREAD_COUNT) {
            throw new Exception("Invalid number of thread ranges, size must be equal to THREAD_COUNT");
        }

        List<String> startingKeysForTx = new ArrayList<>();
        List<Future<?>> tasks = new ArrayList<>();
        for (int i = 0; i < this.THREAD_COUNT; i++) {
            String[] txRange = txRanges.get(i);
            String txStartKey = getThreadStartingKeyForTx(table, txRange[0], txRange[1]);
            startingKeysForTx.add(txStartKey);
            logger.info(String.format("Range: %s - %s", txRange[0], txRange[1]));
            logger.info("Starting key for thread " + i + " is " + txStartKey);
        }

        for (int i = 0; i < this.THREAD_COUNT; i++) {
            logger.info("Getting starting key for thread " + i);
            String startRow = checkpoints.getOrDefault(i, startingKeysForTx.get(i));
            if (startRow == null) {
                logger.severe("Starting key is null for thread " + i + " skipping");
                if (table.equals("tx-by-addr")) {
                    continue;
                } else {
                    throw new Exception("There should be a starting key for tx table");
                }
            }

            boolean isCheckpointStart = checkpoints.get(i) != null;
            logger.info("isCheckpointStart: " + isCheckpointStart);
            String endRow;
            if (i == this.THREAD_COUNT - 1) {
                if (table.equals("tx")) {
                    endRow = this.TX_LAST_KEY;
                } else {
                    endRow = this.TX_BY_ADDR_LAST_KEY;
                }
            } else {
                endRow = startingKeysForTx.get(i + 1);
                if (table.equals("tx-by-addr") && startingKeysForTx.get(i + 1) == null) {
                    endRow = this.TX_BY_ADDR_LAST_KEY;
                }
            }

            logger.info(String.format("Table: %s, Range: %s - %s", table, startRow, endRow));
            tasks.add(runTaskOnWorkerThread(i, table, startRow, endRow, isCheckpointStart));
        }

        return tasks;
    }

    private String getThreadStartingKeyForTx(String tableName, String prefix, String maxPrefix) throws Exception {
        if (tableName == null || prefix == null || maxPrefix == null) {
            throw new IllegalArgumentException("Table name, prefix, and maxPrefix cannot be null");
        }

        if (!tableName.equals("tx-by-addr") && !tableName.equals("tx")) {
            throw new IllegalArgumentException("Invalid table name: " + tableName + ". Must be 'tx' or 'tx-by-addr'");
        }

        try {
            int prefixValue = prefix.charAt(0);
            int maxPrefixValue = maxPrefix.charAt(0);
            for (int i = prefixValue; i <= maxPrefixValue; i++) {
                Query query = Query.create(TableId.of(tableName)).prefix(String.valueOf((char) i)).limit(1);
                List<Row> rows = dataClient.readRowsCallable().all().call(query);
                if (rows.size() > 0) {
                    return rows.get(0).getKey().toStringUtf8();
                }
            }
        } catch (Exception e) {
            throw new Exception("Error getting starting key for thread " + prefix, e);
        }
        return null;
    }

    private Future<?> runTaskOnWorkerThread(int threadId, String tableName, String startRowKey, String endRowKey,
                                               boolean isCheckpointStart) {
        return executorService.submit(() -> splitRangeAndChainUploads(threadId, tableName, startRowKey, endRowKey,
                        !isCheckpointStart));
    }

    private void splitRangeAndChainUploads(int threadId, String tableName, String currentStartRow,
                                           String endRowKey, boolean includeStartRow) {

        if (currentStartRow.compareTo(endRowKey) >= 0) {
            return;
        }

        logger.info(String.format("Queueing task for thread %s, table %s, range %s - %s", threadId, tableName,
                currentStartRow, endRowKey));
        try {
            while (currentStartRow.compareTo(endRowKey) < 0) {
                String currentEndRow = fetchBatch(tableName, currentStartRow, endRowKey, includeStartRow,
                        0);
                if (currentEndRow == null) {
                    // empty batch, we're done
                    logger.info(String.format("[%s] - empty batch for %s - %s", threadId, currentStartRow, currentEndRow));
                    return;
                }

                logger.info(
                        String.format("[%s] - Processed batch %s - %s", threadId, currentStartRow, currentEndRow));
                updateCheckpoint(threadId, currentEndRow, tableName);
                currentStartRow = currentEndRow;
                includeStartRow = false;
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, String.format("Error processing range %s - %s in table %s",
                    currentStartRow, endRowKey, tableName), e);
            e.printStackTrace();
        }
    }

    private String fetchBatch(String tableName, String startRowKey, String endRowKey,
                              boolean includeStartRow, int retryCount) throws IOException {
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
                customWriter.append(rowKey, row);
                lastRow = row;
            }
            customWriter.close();

            logger.info(String.format("Finished after %d rows in fetch batch for %s - %s", rows, startRowKey, endRowKey));

            try {
                outputStream.getUploadFuture().join();
                logger.info(String.format("Finished upload for fetch batch for %s - %s", rows, startRowKey, endRowKey));
            } catch (CosServiceException e) {
                e.printStackTrace();
                return fetchBatch(tableName, startRowKey, endRowKey, includeStartRow, retryCount + 1);
            } catch (CosClientException e) {
                e.printStackTrace();
                return fetchBatch(tableName, startRowKey, endRowKey, includeStartRow, retryCount + 1);
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

        CustomS3FSDataOutputStream customFSDataOutputStream = new CustomS3FSDataOutputStream(
                Paths.get("output/sequencefile/" + tableName + "/range_" + startRowKey + "_" + endRowKey), tableName,
                SYNC_TYPE);

        return customFSDataOutputStream;
    }

    private void updateCheckpoint(int threadId, String endRowKey, String tableName) {
        checkpoints.put(threadId, endRowKey);
        saveCheckpoint(threadId, endRowKey, tableName);
    }

    private void saveCheckpoint(int threadId, String endRowKey, String tableName) {
        try {
            Path tableDir = Paths.get("checkpoints/" + tableName);
            if (!Files.exists(tableDir)) {
                Files.createDirectories(tableDir);
            }
            Path checkpointFile = tableDir.resolve("checkpoint_" + threadId + ".txt");
            Files.write(checkpointFile, endRowKey.getBytes(), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception e) {
            logger.severe(String.format("Error saving checkpoint for thread %s - %s", threadId, e));
        }
    }

    private void loadCheckpoints(String tableName) {
        for (int i = 0; i < this.THREAD_COUNT; i++) {
            Path checkpointPath = Paths.get("/app/checkpoints/" + tableName + "/checkpoint_" + i + ".txt");
            if (Files.exists(checkpointPath)) {
                try {
                    String checkpoint = new String(Files.readAllBytes(checkpointPath));
                    checkpoints.put(i, checkpoint);
                } catch (Exception e) {
                    logger.severe(String.format("Error loading checkpoint for thread %s - %s", i, e));
                }
            }
        }
    }

    public List<String[]> splitHexRange(String startKey, String lastKey) {
        BigInteger start = new BigInteger(startKey, 16);
        BigInteger end = new BigInteger(lastKey, 16);

        start = start.divide(BigInteger.valueOf(this.SUBRANGE_SIZE))
                .multiply(BigInteger.valueOf(this.SUBRANGE_SIZE));

        BigInteger totalRange = end.subtract(start).add(BigInteger.ONE);
        BigInteger intervalSize = totalRange.divide(BigInteger.valueOf(this.THREAD_COUNT));
        BigInteger remainder = totalRange.mod(BigInteger.valueOf(this.THREAD_COUNT));

        List<String[]> intervals = new ArrayList<>();
        BigInteger currentStart = start;

        for (int i = 0; i < this.THREAD_COUNT; i++) {
            currentStart = currentStart.divide(BigInteger.valueOf(this.SUBRANGE_SIZE))
                    .multiply(BigInteger.valueOf(this.SUBRANGE_SIZE));

            BigInteger currentEnd = currentStart.add(intervalSize).subtract(BigInteger.ONE);

            if (remainder.compareTo(BigInteger.ZERO) > 0) {
                currentEnd = currentEnd.add(BigInteger.ONE);
                remainder = remainder.subtract(BigInteger.ONE);
            }

            currentEnd = currentEnd.divide(BigInteger.valueOf(this.SUBRANGE_SIZE))
                    .multiply(BigInteger.valueOf(this.SUBRANGE_SIZE))
                    .add(BigInteger.valueOf(this.SUBRANGE_SIZE - 1));

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

    public List<String[]> splitRangeTx() {
        List<String[]> intervals = new ArrayList<>();
        int totalChars = this.CHARACTERS.length;
        int baseIntervalSize = totalChars / this.THREAD_COUNT;
        int remainingChars = totalChars % this.THREAD_COUNT;

        int currentIndex = 0;
        for (int i = 0; i < this.THREAD_COUNT; i++) {
            int intervalSize = baseIntervalSize + (i < remainingChars ? 1 : 0);
            int endIndex = currentIndex + intervalSize - 1;
            String startKey = String.valueOf(this.CHARACTERS[currentIndex]);
            String endKey = String.valueOf(this.CHARACTERS[endIndex]);
            intervals.add(new String[] { startKey, endKey });
            currentIndex = endIndex + 1;
        }
        return intervals;
    }
}
