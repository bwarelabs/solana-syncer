package com.bwarelabs;

import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.slf4j.LoggerFactory;
import com.qcloud.cos.exception.*;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.stub.metrics.NoopMetricsProvider;

import solana.storage.ConfirmedBlock.ConfirmedBlockOuterClass;
import solana.storage.ConfirmedBlock.ConfirmedBlockOuterClass.ConfirmedBlock;
import org.bitcoinj.core.Base58;

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

import com.google.cloud.bigtable.data.v2.stub.metrics.NoopMetricsProvider;

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

        String projectId = Utils.getRequiredProperty(properties, "bigtable.project-id");
        String instanceId = Utils.getRequiredProperty(properties, "bigtable.instance-id");
        String pathToCredentials = Utils.getRequiredProperty(properties, "bigtable.credentials");
        // Load credentials from JSON key file
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(pathToCredentials));

        BigtableDataSettings.Builder settingsBuilder = BigtableDataSettings.newBuilder().setProjectId(projectId)
                .setInstanceId(instanceId).setAppProfileId("default")
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .setMetricsProvider(NoopMetricsProvider.INSTANCE).setRefreshingChannel(false);

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

    private void startFetchBatch(String tableName, String currentStartRow, String endRowKey) {
        long threadId = Thread.currentThread().getId();
        logger.info(String.format("Thread %s starting task for table %s, range %s - %s", threadId, tableName,
                currentStartRow, endRowKey));

//        if (currentStartRow.compareTo(endRowKey) >= 0) {
//            logger.info(String.format("Invalid range %s - %s", currentStartRow, endRowKey));
//            return;
//        }

        try {
            fetchBatch(tableName, currentStartRow, endRowKey, 0);
//            if (currentEndRow == null) {
//                // empty batch, we're done
//                logger.info(String.format("Empty batch for %s - %s", currentStartRow, currentEndRow));
//                return;
//            }

            logger.info(String.format("[%s] - Processed batch %s - %s", threadId, currentStartRow, endRowKey));
            updateUploadedRanges(currentStartRow, endRowKey, tableName);
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                    String.format("Error processing range %s - %s in table %s", currentStartRow, endRowKey, tableName),
                    e);
            e.printStackTrace();
        }
    }

    private void fetchBatch(String tableName, String startRowKey, String endRowKey, int retryCount)
            throws IOException {
        if (retryCount > 1) {
            throw new IOException("Failed to fetch batch after 2 retries");
        }

        if (tableName.equals("blocks") || tableName.equals("entries")) {
            BigInteger nonFormattedEndRowKey = new BigInteger(startRowKey, 16)
                    .add(BigInteger.valueOf(this.SUBRANGE_SIZE)).subtract(BigInteger.ONE);
            endRowKey = this.formatHex(nonFormattedEndRowKey);
        }

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setStrings("io.serializations", ResultSerialization.class.getName(),
                WritableSerialization.class.getName());

        try (CustomS3FSDataOutputStream blocksOutputStream = getS3OutputStream(tableName, startRowKey, endRowKey);
                CustomSequenceFileWriter blocksCustomWriter = new CustomSequenceFileWriter(hadoopConfig,
                        blocksOutputStream)

//                CustomS3FSDataOutputStream txByAddrOutputStream = getS3OutputStream("tx-by-addr", startRowKey,
//                        endRowKey);
//                CustomSequenceFileWriter txByAddrCustomWriter = new CustomSequenceFileWriter(hadoopConfig,
//                        txByAddrOutputStream);
//
//                CustomS3FSDataOutputStream txOutputStream = getS3OutputStream("tx", startRowKey, endRowKey);
//                CustomSequenceFileWriter txCustomWriter = new CustomSequenceFileWriter(hadoopConfig, txOutputStream)
            ) {
            logger.info(String.format("Before fetch batch for %s - %s", startRowKey, endRowKey));
            ByteStringRange range = ByteStringRange.unbounded().startClosed(startRowKey).endClosed(endRowKey);
            Query query = Query.create(TableId.of(tableName)).range(range).limit(SUBRANGE_SIZE);

            int rows = 0;
            final Set<String> txKeys = new TreeSet<>();
            final Set<String> txByAddrKeys = new TreeSet<>();
            for (Row row : dataClient.readRows(query)) {
                rows++;
                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(row.getKey().toByteArray());

//                RowCell firstCell = row.getCells().iterator().next();
//                if (!firstCell.getQualifier().toStringUtf8().equals("proto")) {
//                    continue;
//                }
//
//                row.getCells().forEach(cell -> {
//                    assert (cell.getQualifier().toStringUtf8().equals("proto"));
//                    try {
//                        InputStream input = cell.getValue().newInput();
//                        // highly unorthodox but there is no bincode implementation for java
//                        // so we rely on the first 4 bytes being an encoding of an enum
//                        byte[] decompressMethod = input.readNBytes(4);
//                        ConfirmedBlock block = null;
//                        if (decompressMethod[0] == 0) {
//                            // no compression
//                            block = ConfirmedBlock.parseFrom(input);
//                        } else if (decompressMethod[0] == 1) {
//                            // bzip2
//                            try (BZip2CompressorInputStream decompressor = new BZip2CompressorInputStream(
//                                    input)) {
//                                block = ConfirmedBlock.parseFrom(decompressor);
//                            }
//                        } else if (decompressMethod[0] == 2) {
//                            // gzip
//                            try (GzipCompressorInputStream decompressor = new GzipCompressorInputStream(
//                                    input)) {
//                                block = ConfirmedBlock.parseFrom(decompressor);
//                            }
//                        } else {
//                            // zstd
//                            assert (decompressMethod[0] == 3);
//                            try (ZstdInputStream decompressor = new ZstdInputStream(input)) {
//                                block = ConfirmedBlock.parseFrom(decompressor);
//                            }
//                        }
//                        // System.out.println("got block " + block.getBlockhash());
//
//                        HashSet<ByteString> accounts = new HashSet<>();
//                        List<ConfirmedBlockOuterClass.ConfirmedTransaction> transactionList = block
//                                .getTransactionsList();
//                        for (ConfirmedBlockOuterClass.ConfirmedTransaction confirmedTransaction : transactionList) {
//                            ConfirmedBlockOuterClass.Transaction transaction = confirmedTransaction.getTransaction();
//
//                            String txKey = Base58.encode(transaction.getSignatures(0).toByteArray());
//                            txKeys.add(txKey);
//                            accounts.addAll(transaction.getMessage().getAccountKeysList());
//                        }
//
//                        long slot = Long.parseLong(row.getKey().toStringUtf8(), 16);
//                        for (ByteString account : accounts) {
//                            String base58Account = String.format("%s/%016x", Base58.encode(account.toByteArray()),
//                                    ~slot);
//                            txByAddrKeys.add(base58Account);
//                        }
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                });
                blocksCustomWriter.append(rowKey, row);

            }
//            fetchAndWriteData(txCustomWriter, TableId.of("tx"), txKeys);
//            fetchAndWriteData(txByAddrCustomWriter, TableId.of("tx-by-addr"), txByAddrKeys);
            blocksCustomWriter.close();
//            txCustomWriter.close();
//            txByAddrCustomWriter.close();

            logger.info(
                    String.format("Finished after %d rows in fetch batch for %s - %s", rows, startRowKey, endRowKey));

            try {
                blocksOutputStream.getUploadFuture().join();
//                txByAddrOutputStream.getUploadFuture().join();
//                txOutputStream.getUploadFuture().join();
                logger.info(String.format("Finished upload for fetch batch for %s - %s", startRowKey, endRowKey));
            } catch (CosClientException e) {
                e.printStackTrace();
                fetchBatch(tableName, startRowKey, endRowKey, retryCount + 1);
            }
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
        } catch (IOException e) {
            logger.severe("Error loading checkpoints for table " + tableName);
            e.printStackTrace();
        }
    }

    public List<String[]> splitHexRange(String startKey, String lastKey) {
        BigInteger start = new BigInteger(startKey, 16);
        BigInteger end = new BigInteger(lastKey, 16);

        // Align start down to the nearest subrange in order to get multiples of subrange size
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
