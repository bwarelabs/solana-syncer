package com.bwarelabs;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.Properties;

public class BigTableToCosWriter {
    private static final Logger logger = Logger.getLogger(BigTableToCosWriter.class.getName());
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BigTableToCosWriter.class);

    private final Connection connection;
    private final ExecutorService executorService;
    private final int THREAD_COUNT;
    private final int SUBRANGE_SIZE; // Number of rows to process in each batch within a thread range
    private final int BATCH_LIMIT; // Limit the number of chained batches
    private final String TX_LAST_KEY;
    private final String TX_BY_ADDR_LAST_KEY;
    private final String BLOCKS_LAST_KEY;
    private final String ENTRIES_LAST_KEY;
    private final List<CompletableFuture<Void>> allUploadFutures = new ArrayList<>();
    private final Map<Integer, String> checkpoints = new HashMap<>();
    private final AtomicReference<CompletableFuture<AsyncConnection>> future = new AtomicReference<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Configuration configuration;

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
        this.ENTRIES_LAST_KEY = Utils.getRequiredProperty(properties, "bigtable.entries-last-key");

        String projectId = Utils.getRequiredProperty(properties, "bigtable.project-id");
        String instanceId = Utils.getRequiredProperty(properties, "bigtable.instance-id");
        String pathToCredentials = Utils.getRequiredProperty(properties, "bigtable.credentials");

        this.configuration = BigtableConfiguration.configure(projectId, instanceId);
        this.configuration.set(BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY,
                pathToCredentials);

        connection = BigtableConfiguration.connect(configuration);
        executorService = Executors.newFixedThreadPool(this.THREAD_COUNT);
        loadCheckpoints();
    }

    public void write(String tableName) throws Exception {
        logger.info("Starting BigTable to COS writer");

        if (tableName == null || tableName.trim().isEmpty()) {
            logger.severe("Table name cannot be null or empty");
            return;
        }

        if (tableName.equals("blocks") || tableName.equals("entries")) {
            writeBlocksOrEntries(tableName);
        } else if (tableName.equals("tx") || tableName.equals("tx-by-addr")) {
            writeTx(tableName);
        } else {
            logger.severe("Invalid table name: " + tableName);
        }

        logger.info("BigTable to COS writer completed");
        connection.close();
        closeConnections();
        shutdownExecutorService();
    }

    private void writeBlocksOrEntries(String table) throws Exception {
        logger.info(String.format("Starting BigTable to COS writer for table '%s'", table));

        List<String[]> hexRanges = this
                .splitHexRange(table.equals("blocks") ? this.BLOCKS_LAST_KEY : this.ENTRIES_LAST_KEY);
        if (hexRanges.size() != this.THREAD_COUNT) {
            throw new Exception("Invalid number of thread ranges, size must be equal to THREAD_COUNT");
        }

        for (int i = 0; i < this.THREAD_COUNT; i++) {
            String[] hexRange = hexRanges.get(i);
            String startRow = checkpoints.getOrDefault(i, hexRange[0]);
            String endRow = hexRange[1];

            boolean isCheckpointStart = checkpoints.get(i) != null;

            logger.info(String.format("Table: %s, Range: %s - %s", table, startRow, endRow));
            runTaskOnWorkerThread(i, table, startRow, endRow, isCheckpointStart);
        }

        CompletableFuture<Void> allUploads = CompletableFuture
                .allOf(allUploadFutures.toArray(new CompletableFuture[0]));
        allUploads.join();
        logger.info(String.format("Table '%s' processed and uploaded.", table));
    }

    private void writeTx(String table) throws Exception {
        logger.info(String.format("Starting BigTable to COS writer for table '%s'", table));

        List<String[]> txRanges = this.splitRangeTx();
        if (txRanges.size() != this.THREAD_COUNT) {
            throw new Exception("Invalid number of thread ranges, size must be equal to THREAD_COUNT");
        }

        List<String> startingKeysForTx = new ArrayList<>();
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
                    // this can happen if the key distribution is not uniform, I was not able to
                    // check this
                    // in the prod bigtable
                    continue;
                } else {
                    // this should not happen because even on local emulator, the keys are pretty
                    // uniformly distributed
                    throw new Exception("There should be a starting key for tx table");
                }
            }

            boolean isCheckpointStart = checkpoints.get(i) != null;
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
            runTaskOnWorkerThread(i, table, startRow, endRow, isCheckpointStart);
        }

        CompletableFuture<Void> allUploads = CompletableFuture
                .allOf(allUploadFutures.toArray(new CompletableFuture[0]));
        allUploads.join();
        logger.info(String.format("Table '%s' processed and uploaded.", table));
    }

    private String getThreadStartingKeyForTx(String tableName, String prefix, String maxPrefix) throws Exception {
        if (tableName == null || prefix == null || maxPrefix == null) {
            throw new IllegalArgumentException("Table name, prefix, and maxPrefix cannot be null");
        }

        if (!tableName.equals("tx-by-addr") && !tableName.equals("tx")) {
            throw new IllegalArgumentException("Invalid table name: " + tableName + ". Must be 'tx' or 'tx-by-addr'");
        }

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            int prefixValue = prefix.charAt(0);
            int maxPrefixValue = maxPrefix.charAt(0);
            for (int i = prefixValue; i <= maxPrefixValue; i++) {
                Scan scan = new Scan();
                scan.setStartStopRowForPrefixScan(Bytes.toBytes(String.valueOf((char) i)));
                scan.setLimit(1);
                try (ResultScanner scanner = table.getScanner(scan)) {
                    Result next = scanner.next();
                    if (next != null) {
                        return Bytes.toString(next.getRow());
                    }
                }
            }
        } catch (Exception e) {
            throw new Exception("Error getting starting key for thread " + prefix, e);
        }
        return null;
    }

    private void runTaskOnWorkerThread(int threadId, String tableName, String startRowKey, String endRowKey,
            boolean isCheckpointStart) {
        CompletableFuture<Void> processingFuture = CompletableFuture
                .supplyAsync(() -> splitRangeAndChainUploads(threadId, tableName, startRowKey, endRowKey,
                        !isCheckpointStart, new BatchCounter(), new ArrayList<>())
                        .exceptionally(e -> {
                            logger.severe(String.format("Error processing table range for %s - %s", tableName, e));
                            throw new RuntimeException(e);
                        }), executorService)
                .thenCompose(batchFuture -> batchFuture);

        allUploadFutures.add(processingFuture);
    }

    private CompletableFuture<Void> splitRangeAndChainUploads(int threadId, String tableName, String currentStartRow,
            String endRowKey, boolean includeStartRow, BatchCounter batchCounter,
            List<CompletableFuture<Void>> batchUploadFutures) {
        if (currentStartRow.compareTo(endRowKey) == 0) {
            CompletableFuture<Void> allUploads = CompletableFuture
                    .allOf(batchUploadFutures.toArray(new CompletableFuture[0]));

            return allUploads.thenCompose(v -> {
                updateCheckpoint(threadId, endRowKey, tableName);
                return CompletableFuture.completedFuture(null);
            });
        }

        return fetchBatch(tableName, currentStartRow, endRowKey, includeStartRow)
                .thenCompose(batch -> {
                    if (batch.isEmpty()) {
                        logger.info(String.format("[%s] Empty batch for startRow: %s and endRow: %s",
                                Thread.currentThread().getName(), currentStartRow, endRowKey));
                        updateCheckpoint(threadId, endRowKey, tableName);

                        CompletableFuture<Void> allUploads = CompletableFuture
                                .allOf(batchUploadFutures.toArray(new CompletableFuture[0]));
                        return allUploads.thenCompose(v -> CompletableFuture.completedFuture(null));
                    }

                    String rangeStartRow = Bytes.toString(batch.get(0).getRow());
                    logger.info(String.format("[%s] Batch size: %s for startRow: %s and endRow: %s",
                            Thread.currentThread().getName(), batch.size(), rangeStartRow, endRowKey));
                    String rangEndRow = Bytes.toString(batch.get(batch.size() - 1).getRow());

                    CompletableFuture<Void> uploadFuture = uploadAsync(tableName, rangeStartRow, rangEndRow, batch);
                    batchUploadFutures.add(uploadFuture);

                    if (batchCounter.incrementAndGet() >= BATCH_LIMIT) {
                        batchCounter.reset();
                        return CompletableFuture.allOf(batchUploadFutures.toArray(new CompletableFuture[0]))
                                .thenCompose(v -> {
                                    updateCheckpoint(threadId, rangEndRow, tableName);
                                    return splitRangeAndChainUploads(threadId, tableName, rangEndRow, endRowKey, false,
                                            batchCounter, new ArrayList<>());
                                });
                    }

                    return splitRangeAndChainUploads(threadId, tableName, rangEndRow, endRowKey, false, batchCounter,
                            batchUploadFutures);
                });
    }

    private CompletableFuture<Void> uploadAsync(String tableName, String startRow, String endRow, List<Result> batch) {
        CompletableFuture<Void> uploadFuture = CompletableFuture.completedFuture(null);
        try {
            CustomS3FSDataOutputStream customFSDataOutputStream = convertToSeqAndStartUpload(tableName, startRow,
                    endRow, batch);
            logger.info(
                    String.format("[%s] Processing batch %s - %s", Thread.currentThread().getName(), startRow, endRow));
            uploadFuture = customFSDataOutputStream.getUploadFuture()
                    .thenRun(() -> logger.info(String.format("[%s] Successfully uploaded %s to COS",
                            Thread.currentThread().getName(), customFSDataOutputStream.getS3Key())));
        } catch (Exception e) {
            logger.severe(String.format("[%s] Error converting batch to sequence file format for %s - %s in table %s",
                    Thread.currentThread().getName(), startRow, endRow, tableName));
        }

        return uploadFuture;
    }

    private CompletableFuture<List<Result>> fetchBatch(String tableName, String startRowKey, String endRowKey,
            boolean includeStartRow) {
        CompletableFuture<List<Result>> resultFuture = new CompletableFuture<>();

        CompletableFuture<AsyncConnection> future = getConn();
        addListener(future, (conn, error) -> {
            if (error != null) {
                logger.severe(String.format("Error getting connection for %s - %s", tableName, error));
                resultFuture.completeExceptionally(error);
                return;
            }

            AsyncTable<?> table = conn.getTable(TableName.valueOf(tableName), executorService);
            if (startRowKey.equals(endRowKey)) {
                // if you call scan with the same start and end row, it will return the whole
                // table
                Get get = new Get(Bytes.toBytes(startRowKey));

                addListener(table.get(get), (getResp, getErr) -> {
                    if (getErr != null) {
                        logger.severe(String.format("Error getting row %s - %s", startRowKey, getErr));
                        resultFuture.completeExceptionally(getErr);
                        return;
                    }

                    List<Result> batch = new ArrayList<>();
                    if (!getResp.isEmpty()) {
                        batch.add(getResp);
                    }
                    resultFuture.complete(batch);
                });
            } else {
                Scan scan = new Scan()
                        .withStartRow(Bytes.toBytes(startRowKey), includeStartRow)
                        .withStopRow(Bytes.toBytes(endRowKey), true)
                        .setCaching(SUBRANGE_SIZE)
                        .setLimit(SUBRANGE_SIZE);

                addListener(table.scanAll(scan), (scanResp, scanErr) -> {
                    if (scanErr != null) {
                        logger.severe(String.format("Error scanning table %s - %s", tableName, scanErr));
                        resultFuture.completeExceptionally(scanErr);
                        return;
                    }

                    List<Result> batch = new ArrayList<>(scanResp);
                    resultFuture.complete(batch);
                });
            }
        });

        return resultFuture;
    }

    private CustomS3FSDataOutputStream convertToSeqAndStartUpload(String tableName, String startRowKey,
            String endRowKey, List<Result> batch) throws IOException {
        logger.info(String.format("[%s] Converting batch to sequence file format for %s from %s to %s",
                Thread.currentThread().getName(), tableName, startRowKey, endRowKey));

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setStrings(
                "io.serializations",
                ResultSerialization.class.getName(),
                WritableSerialization.class.getName());

        RawLocalFileSystem fs = new RawLocalFileSystem();
        fs.setConf(hadoopConfig);

        if (tableName.equals("tx-by-addr")) {
            startRowKey = startRowKey.replace("/", "_");
            endRowKey = endRowKey.replace("/", "_");
        }

        CustomS3FSDataOutputStream customFSDataOutputStream = new CustomS3FSDataOutputStream(
                Paths.get("output/sequencefile/" + tableName + "/range_" + startRowKey + "_" + endRowKey), tableName);
        CustomSequenceFileWriter customWriter = null;

        try {
            customWriter = new CustomSequenceFileWriter(hadoopConfig, customFSDataOutputStream);

            for (Result result : batch) {
                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(result.getRow());
                customWriter.append(rowKey, result);
            }
        } finally {
            if (customWriter != null) {
                logger.info(String.format("[%s] Closing sequence file writer for %s from %s to %s",
                        Thread.currentThread().getName(), tableName, startRowKey, endRowKey));
                customWriter.close();
            }
        }

        return customFSDataOutputStream;
    }

    private void updateCheckpoint(int threadId, String endRowKey, String tableName) {
        checkpoints.put(threadId, endRowKey);
        saveCheckpoint(threadId, endRowKey, tableName);
    }

    private void saveCheckpoint(int threadId, String endRowKey, String tableName) {
        try {
            Path tableDir = Paths.get(tableName);
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

    private void loadCheckpoints() {
        for (int i = 0; i < this.THREAD_COUNT; i++) {
            Path checkpointPath = Paths.get("checkpoint_" + i + ".txt");
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

    public List<String[]> splitHexRange(String lastKey) {
        BigInteger start = new BigInteger("0000000000000000", 16);
        BigInteger end = new BigInteger(lastKey, 16);

        BigInteger totalRange = end.subtract(start).add(BigInteger.ONE); // +1 to include the end in the range
        BigInteger intervalSize = totalRange.divide(BigInteger.valueOf(this.THREAD_COUNT));
        BigInteger remainder = totalRange.mod(BigInteger.valueOf(this.THREAD_COUNT));

        List<String[]> intervals = new ArrayList<>();
        BigInteger currentStart = start;

        for (int i = 0; i < this.THREAD_COUNT; i++) {
            BigInteger currentEnd = currentStart.add(intervalSize).subtract(BigInteger.ONE);
            if (remainder.compareTo(BigInteger.ZERO) > 0) {
                currentEnd = currentEnd.add(BigInteger.ONE);
                remainder = remainder.subtract(BigInteger.ONE);
            }
            intervals.add(new String[] {
                    this.formatHex(currentStart),
                    this.formatHex(currentEnd)
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

    private void closeConnections() {
        CompletableFuture<AsyncConnection> connFuture = future.get();
        if (connFuture != null && !isClosed.getAndSet(true)) {
            connFuture.thenAccept(conn -> {
                try {
                    conn.close();
                    logger.info("AsyncConnection closed successfully.");
                } catch (IOException e) {
                    logger.severe("Error closing AsyncConnection: " + e.getMessage());
                }
            }).exceptionally(e -> {
                logger.severe("Error during AsyncConnection closure: " + e.getMessage());
                return null;
            });
        }
    }

    private void shutdownExecutorService() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS))
                    logger.severe("Executor service did not terminate");
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private CompletableFuture<AsyncConnection> getConn() {
        CompletableFuture<AsyncConnection> f = future.get();
        if (f != null) {
            return f;
        }
        for (;;) {
            if (future.compareAndSet(null, new CompletableFuture<>())) {
                CompletableFuture<AsyncConnection> toComplete = future.get();
                addListener(ConnectionFactory.createAsyncConnection(this.configuration), (conn, error) -> {
                    if (error != null) {
                        toComplete.completeExceptionally(error);
                        // reset the future holder so we will get a chance to recreate an async
                        // connection at next try.
                        future.set(null);
                        return;
                    }
                    toComplete.complete(conn);
                });
                return toComplete;
            } else {
                f = future.get();
                if (f != null) {
                    return f;
                }
            }
        }
    }

    private static class BatchCounter {
        private int count = 0;

        int incrementAndGet() {
            return ++count;
        }

        void reset() {
            count = 0;
        }
    }
}
