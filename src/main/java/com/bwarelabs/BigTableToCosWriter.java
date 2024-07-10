package com.bwarelabs;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
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

    private final Connection connection;
    private final ExecutorService executorService;
    private final int THREAD_COUNT;
    private final int SUBRANGE_SIZE; // Number of rows to process in each batch within a thread range
    private final int BATCH_LIMIT;  // Limit the number of chained batches
    private final String TX_LAST_KEY;
    private final String TX_BY_ADDR_LAST_KEY;
    private final String HEX_LAST_KEY;
    private final List<CompletableFuture<Void>> allUploadFutures = new ArrayList<>();
    private final Map<Integer, String> checkpoints = new HashMap<>();
    private final AtomicReference<CompletableFuture<AsyncConnection>> future =
            new AtomicReference<>();
    private final Configuration configuration;

    private final char[] CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

    public BigTableToCosWriter(Properties properties) throws IOException {
        LogManager.getLogManager().readConfiguration(
                BigTableToCosWriter.class.getClassLoader().getResourceAsStream("logging.properties"));

        this.THREAD_COUNT = Integer.parseInt(Utils.getRequiredProperty(properties, "bigtable.thread-count"));
        this.SUBRANGE_SIZE = Integer.parseInt(Utils.getRequiredProperty(properties, "bigtable.subrange-size"));
        this.BATCH_LIMIT = Integer.parseInt(Utils.getRequiredProperty(properties,"bigtable.batch-limit"));
        this.TX_LAST_KEY = Utils.getRequiredProperty(properties, "bigtable.tx-last-key");
        this.TX_BY_ADDR_LAST_KEY = Utils.getRequiredProperty(properties, "bigtable.tx-by-addr-last-key");
        this.HEX_LAST_KEY = Utils.getRequiredProperty(properties, "bigtable.hex-last-key");

        this.configuration = BigtableConfiguration.configure("emulator", "solana-ledger");
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
    }

    private void writeBlocksOrEntries(String table) throws Exception {
        logger.info(String.format("Starting BigTable to COS writer for table '%s'", table));

        List<String[]> hexRanges = this.splitHexRange();
        if (hexRanges.size() != this.THREAD_COUNT) {
            throw new Exception("Invalid number of thread ranges, size must be equal to THREAD_COUNT");
        }

        for (int i = 0; i < this.THREAD_COUNT; i++) {
            String[] hexRange = hexRanges.get(i);
            String startRow = checkpoints.getOrDefault(i, hexRange[0]);
            String endRow = hexRange[1];
            logger.info(String.format("Table: %s, Range: %s - %s", table, startRow, endRow));
            processTableRange(i, table, startRow, endRow);
        }

        CompletableFuture<Void> allUploads = CompletableFuture.allOf(allUploadFutures.toArray(new CompletableFuture[0]));
        allUploads.join();
        executorService.shutdown();
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
            String txStartKey = getThreadStartingKey(table, txRange[0], txRange[1]);
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
            String endRow;
            if (i == this.THREAD_COUNT - 1) {
                endRow = this.TX_LAST_KEY;
            } else {
                endRow = startingKeysForTx.get(i + 1);
                if (table.equals("tx-by-addr") && startingKeysForTx.get(i + 1) == null) {
                    endRow = this.TX_BY_ADDR_LAST_KEY;
                }
            }

            logger.info(String.format("Table: %s, Range: %s - %s", table, startRow, endRow));
            processTableRangeTx(i, table, startRow, endRow, isCheckpointStart);
        }

        CompletableFuture<Void> allUploads = CompletableFuture.allOf(allUploadFutures.toArray(new CompletableFuture[0]));
        allUploads.join();
        executorService.shutdown();
        logger.info(String.format("Table '%s' processed and uploaded.", table));
    }

    private String getThreadStartingKey(String tableName, String prefix, String maxPrefix) throws Exception {
        if (tableName == null || prefix == null || maxPrefix == null) {
            throw new IllegalArgumentException("Table name, prefix, and maxPrefix cannot be null");
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

    private void processTableRangeTx(int threadId, String tableName, String startRowKey, String endRowKey, boolean isCheckpointStart) {
        CompletableFuture<Void> processingFuture = processBatchTx(threadId, tableName, startRowKey, endRowKey, !isCheckpointStart, new BatchCounter())
                .exceptionally(e -> {
                    logger.severe(String.format("Error processing table range for %s - %s", tableName, e));
                    throw new RuntimeException(e);
                });

        allUploadFutures.add(processingFuture);
    }

    private CompletableFuture<Void> processBatchTx(int threadId, String tableName, String currentStartRow, String endRowKey, boolean includeStartRow, BatchCounter batchCounter) {
        if (currentStartRow.compareTo(endRowKey) > 0) {
            return CompletableFuture.completedFuture(null);
        }

        return fetchBatchTx(tableName, currentStartRow, endRowKey, includeStartRow)
                .thenCompose(batch -> {
                    if (batch.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    String newStartRow = Bytes.toString(batch.get(0).getRow());
                    logger.info(String.format("[%s] Batch size: %s for startRow: %s and endRow: %s", Thread.currentThread().getName(), batch.size(), newStartRow, endRowKey));
                    String newEndRow = Bytes.toString(batch.get(batch.size() - 1).getRow());

                    List<CompletableFuture<Void>> uploadFutures = getUploadFutures(tableName, newStartRow, newEndRow, batch);

                    if (batchCounter.incrementAndGet() >= BATCH_LIMIT) {
                        batchCounter.reset();
                        return CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0]))
                                .thenCompose(v -> {
                                    updateCheckpoint(threadId, newEndRow, tableName);
                                    return processBatchTx(threadId, tableName, newEndRow, endRowKey, false, batchCounter);
                                });
                    }

                    return processBatchTx(threadId, tableName, newEndRow, endRowKey, false, batchCounter)
                            .thenCompose(v -> CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0])));
                });
    }

    private void processTableRange(int threadId, String tableName, String startRowKey, String endRowKey) {
        CompletableFuture<Void> processingFuture = CompletableFuture.runAsync(() -> {
            try {
                String currentStartRow = startRowKey;
                List<CompletableFuture<Void>> batchFutures = new ArrayList<>();

                while (currentStartRow.compareTo(endRowKey) <= 0) {
                    List<CompletableFuture<Void>> currentBatchFutures = new ArrayList<>();
                    CompletableFuture<Void> rangeFuture = CompletableFuture.completedFuture(null);

                    for (int i = 0; i < BATCH_LIMIT && currentStartRow.compareTo(endRowKey) <= 0; i++) {
                        String nextStartRow = currentStartRow;
                        String currentEndRow = calculateEndRow(currentStartRow, endRowKey);

                        rangeFuture = rangeFuture.thenCompose(v -> fetchBatch(tableName, nextStartRow, currentEndRow))
                                .thenCompose(batch -> {
                                    logger.info(String.format("[%s] Batch size: %s for startRow: %s and endRow: %s",
                                            Thread.currentThread().getName(), batch.size(), nextStartRow, currentEndRow));

                                    if (!batch.isEmpty()) {
                                        List<CompletableFuture<Void>> uploadFutures = getUploadFutures(tableName, nextStartRow, currentEndRow, batch);
                                        currentBatchFutures.addAll(uploadFutures);
                                    }

                                    return CompletableFuture.completedFuture(null);
                                });

                        currentStartRow = incrementRowKey(currentEndRow);
                    }

                    CompletableFuture<Void> currentBatch = rangeFuture.thenCompose(v -> CompletableFuture.allOf(currentBatchFutures.toArray(new CompletableFuture[0])));
                    currentBatch.join();

                    batchFutures.add(currentBatch);

                    updateCheckpoint(threadId, currentStartRow, tableName);
                }

                CompletableFuture<Void> allBatches = CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0]));
                allBatches.join();
            } catch (Exception e) {
                logger.severe(String.format("Error processing table range for %s - %s", tableName, e));
                throw new RuntimeException(e);
            }
        }, executorService);

        allUploadFutures.add(processingFuture);
    }

    private List<CompletableFuture<Void>> getUploadFutures(String tableName, String currentStartRow, String currentEndRow, List<Result> batch) {
        List<CompletableFuture<Void>> batchFutures = new ArrayList<>();

        CompletableFuture<Void> uploadFuture;
        try {
            CustomS3FSDataOutputStream customFSDataOutputStream = convertToSeqAndStartUpload(tableName, currentStartRow, currentEndRow, batch);
            logger.info(String.format("[%s] Processing batch %s - %s", Thread.currentThread().getName(), currentStartRow, currentEndRow));
            uploadFuture = customFSDataOutputStream.getUploadFuture().thenRun(() -> logger.info(String.format("[%s] Successfully uploaded %s to COS", Thread.currentThread().getName(), customFSDataOutputStream.getS3Key())));
        } catch (Exception e) {
            logger.severe(String.format("[%s] Error converting batch to sequence file format for %s - %s in table %s", Thread.currentThread().getName(), currentStartRow, currentEndRow, tableName));
            return batchFutures;
        }

        batchFutures.add(uploadFuture);

        return batchFutures;
    }

    private String calculateEndRow(String startRow, String endRow) {
        long start = Long.parseUnsignedLong(startRow, 16);
        long end = Long.parseUnsignedLong(endRow, 16);
        long rangeSize = Math.min(SUBRANGE_SIZE, end - start + 1);
        long newEnd = start + rangeSize - 1;
        return String.format("%016x", newEnd);
    }

    private String incrementRowKey(String rowKey) {
        long row = Long.parseUnsignedLong(rowKey, 16);
        return String.format("%016x", row + 1);
    }

    private CompletableFuture<List<Result>> fetchBatch(String tableName, String startRowKey, String endRowKey) {
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
                        .withStartRow(Bytes.toBytes(startRowKey))
                        .withStopRow(Bytes.toBytes(endRowKey))
                        .setCaching(SUBRANGE_SIZE);

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

    private CompletableFuture<List<Result>> fetchBatchTx(String tableName, String startRowKey, String endRowKey, boolean includeStartRow) {
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
                Scan scan = new Scan().withStartRow(Bytes.toBytes(startRowKey), includeStartRow).withStopRow(Bytes.toBytes(endRowKey), false).setCaching(SUBRANGE_SIZE).setLimit(SUBRANGE_SIZE);

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

    private CustomS3FSDataOutputStream convertToSeqAndStartUpload(String tableName, String startRowKey, String endRowKey, List<Result> batch) throws IOException {
        logger.info(String.format("[%s] Converting batch to sequence file format for %s from %s to %s", Thread.currentThread().getName(), tableName, startRowKey, endRowKey));

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

        CustomS3FSDataOutputStream customFSDataOutputStream = new CustomS3FSDataOutputStream(Paths.get("output/sequencefile/" + tableName + "/range_" + startRowKey + "_" + endRowKey), tableName);
        CustomSequenceFileWriter customWriter = null;

        try {
            customWriter = new CustomSequenceFileWriter(hadoopConfig, customFSDataOutputStream);

            for (Result result : batch) {
                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(result.getRow());
                customWriter.append(rowKey, result);
            }
        } finally {
            if (customWriter != null) {
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
            Files.write(checkpointFile, endRowKey.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
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

    public List<String[]> splitHexRange() {
        BigInteger start = new BigInteger("0000000000000000", 16);
        BigInteger end = new BigInteger(this.HEX_LAST_KEY, 16);

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
            intervals.add(new String[]{
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
            intervals.add(new String[]{startKey, endKey});
            currentIndex = endIndex + 1;
        }
        return intervals;
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
