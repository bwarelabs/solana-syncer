package com.bwarelabs.geyser2cos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.serializer.WritableSerialization;

import com.bwarelabs.common.CosUtils;
import com.bwarelabs.common.CustomS3FSDataOutputStream;
import com.bwarelabs.common.CustomSequenceFileWriter;
import com.bwarelabs.common.Utils;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletionException;

/**
 * GeyserPluginToCosWriter class is responsible for reading data from local
 * storage,
 * processing it and uploading the processed data to Tencent Cloud Object
 * Storage (COS) asynchronously.
 * <p>
 * The main steps performed by this class are:
 * 1. Identify slot range directories from the input storage path.
 * 2. For each slot range directory, process the subdirectories (slots).
 * 3. For each slot, create sequence files for different categories (entries, blocks)
 * 4. Write the data to the sequence files.
 * 5. Upload the sequence files to COS asynchronously.
 * 6. Ensure all uploads are completed before finishing the process.
 * <p>
 * Asynchronous Processing:
 * - The processing of each slot range and the subsequent uploads are handled
 * asynchronously.
 * - CompletableFuture is used to manage asynchronous tasks and ensure that all
 * uploads are completed
 * before the program exits.
 */
public class Writer implements AutoCloseable {
    private final Logger logger = Logger.getLogger(Writer.class.getName());

    private final String SYNC_TYPE;
    private final String STORAGE_PATH;
    private final int THREAD_COUNT;

    private final ExecutorService executorService;
    private final CosUtils cosUtils;

    public Writer(CosUtils cosUtils, Properties properties) {
        this.cosUtils = cosUtils;

        SYNC_TYPE = Utils.getRequiredProperty(properties, "sync.type");
        STORAGE_PATH = Utils.getRequiredProperty(properties, "geyser-plugin.input-directory");
        THREAD_COUNT = Utils.getRequiredIntegerProperty(properties, "geyser-plugin.thread-count");

        executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        logger.info("Reading data from local files from path " + STORAGE_PATH);
    }

    public void watchDirectory() {
        Path path = Path.of(STORAGE_PATH);
        logger.info("Uploading existing directories...");

        Set<String> directoriesToSkip = new HashSet<>(Arrays.asList());

        try {
            // List all directories to process
            List<Path> directories = Files.list(path)
                .filter(Files::isDirectory)
                .filter(dir -> !directoriesToSkip.contains(dir.getFileName().toString()))
                .collect(Collectors.toList());

            // Store all futures to track errors after processing all batches
            List<CompletableFuture<Void>> allFutures = new ArrayList<>();

            // Process directories in batches of 10
            int batchSize = 10;
            for (int i = 0; i < directories.size(); i += batchSize) {
                List<Path> batch = directories.subList(i, Math.min(i + batchSize, directories.size()));

                List<CompletableFuture<Void>> futures = batch.stream()
                    .map(slotRangeDir -> CompletableFuture.supplyAsync(() -> processSlotRange(slotRangeDir), executorService)
                            .thenComposeAsync(f -> {
                                if (f == null) {
                                    logger.severe("processSlotRange returned a null CompletableFuture for slot range: " + slotRangeDir.getFileName());
                                    return CompletableFuture.failedFuture(new RuntimeException("Null CompletableFuture for slot range: " + slotRangeDir.getFileName()));
                                }
                                return f;
                            }, executorService)
                            .exceptionally(ex -> {
                                logger.severe("Exception occurred while processing slot range: " + slotRangeDir.getFileName() + ". " + ex.getMessage());
                                throw new CompletionException(ex);
                            }))
                    .collect(Collectors.toList());

                // Add batch futures to the master list
                allFutures.addAll(futures);

                // Wait for all uploads in the current batch to complete
                CompletableFuture<Void> batchUploads = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                batchUploads.join();
            }

            // Check for failed futures after all batches have completed
            boolean hasFailedFutures = allFutures.stream().anyMatch(future -> {
                try {
                    future.join(); // This will throw an exception if the future failed
                    return false;
                } catch (Exception e) {
                    logger.severe("Exception while checking future's result: " + e.getMessage());
                    e.printStackTrace();
                    return true;
                }
            });

            if (hasFailedFutures) {
                logger.severe("One or more slot ranges failed to process (errored).");
            } else {
                logger.info("All slot ranges processed and uploaded successfully.");
            }

            logger.info("Uploaded all existing directories.");
            logger.info("Starting watch process...");

            // Start watching the directory for new subdirectories
            try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
                path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

                logger.info("Watching directory: " + path);

                while (true) {
                    WatchKey key;
                    key = watchService.take();

                    for (WatchEvent<?> event : key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();

                        if (kind == StandardWatchEventKinds.OVERFLOW) {
                            continue;
                        }

                        @SuppressWarnings("unchecked")
                        WatchEvent<Path> ev = (WatchEvent<Path>) event;

                        Path fileName = ev.context();
                        Path child = path.resolve(fileName);

                        if (Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)) {
                            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> processSlotRange(child),
                                    executorService);
                            allFutures.add(future);
                        }
                    }

                    boolean valid = key.reset();
                    if (!valid) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            CompletableFuture<Void> allUploads = CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]));
            allUploads.thenRun(() -> {
                boolean hasFailedFutures2 = allFutures.stream().anyMatch(future -> {
                    try {
                        future.join(); // This will throw an exception if the future failed
                        return false; 
                    } catch (Exception e) {
                        logger.severe("Exception while checking future's result: " + e.getMessage());
                        e.printStackTrace();
                        return true;
                    }
                });

                if (hasFailedFutures2) {
                    logger.severe("One or more slot ranges failed to process (errored).");
                } else {
                    logger.info("All slot ranges processed and uploaded.");
                }
        }).join();

        } catch (Exception e) {
            logger.severe(String.format("Error processing directory: %s, %s", path, e.getMessage()));
            e.printStackTrace();
        }
    }

    private CompletableFuture<Void> processSlotRange(Path slotRangeDir) {
        logger.info("Processing slot range: " + slotRangeDir.getFileName());

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setStrings("io.serializations", WritableSerialization.class.getName(),
                ResultSerialization.class.getName());
        String range = String.valueOf(slotRangeDir.getFileName());

        try (Stream<Path> slotDirs = Files.list(slotRangeDir)) {
            CustomS3FSDataOutputStream blocksStream = new CustomS3FSDataOutputStream(cosUtils, range, "blocks",
                    SYNC_TYPE);
            CustomSequenceFileWriter blocksWriter = new CustomSequenceFileWriter(hadoopConfig, blocksStream);

            CustomS3FSDataOutputStream entriesStream = new CustomS3FSDataOutputStream(cosUtils, range, "entries",
                    SYNC_TYPE);
            CustomSequenceFileWriter entriesWriter = new CustomSequenceFileWriter(hadoopConfig, entriesStream);

            slotDirs
                    .filter(Files::isDirectory)
                    .forEach(slotDir -> {
                        try {
                            processSlot(slotDir, entriesWriter, blocksWriter);
                        } catch (Exception e) {
                            logger.severe(String.format("Error processing slot: %s, %s", slotDir.getFileName(),
                                    e.getMessage()));
                            e.printStackTrace();
                        }
                    });

            // Closing writers so the CustomS3FSDataOutputStream creates the futures
            blocksWriter.close();
            entriesWriter.close();

            CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(blocksStream.getUploadFuture(), entriesStream.getUploadFuture())
            .thenComposeAsync(result -> {
                if (blocksStream.getUploadFuture().isCompletedExceptionally() || entriesStream.getUploadFuture().isCompletedExceptionally()) {
                    logger.severe("One or more uploads failed; skipping deletion for slot range: " + slotRangeDir.getFileName());
                    return CompletableFuture.failedFuture(new RuntimeException("Upload failed for slot range: " + slotRangeDir.getFileName()));
                }

                return CompletableFuture.runAsync(() -> {
                    logger.info("Slot range processed: " + slotRangeDir.getFileName() + ", deleting slot range...");
                    try {
                        deleteDirectory(slotRangeDir);
                        logger.info("Deleted slot range: " + slotRangeDir.getFileName());
                    } catch (Exception e) {
                        logger.severe(String.format("Error deleting slot range: %s, %s", slotRangeDir.getFileName(), e.getMessage()));
                    }
                }, executorService);
            }, executorService);

            return combinedFuture.exceptionally(ex -> {
                logger.severe("Exception occurred while processing slot range: " + slotRangeDir.getFileName() + ". " + ex.getMessage());
                throw new CompletionException(ex); 
            });
        } catch (Exception e) {
             logger.severe(String.format("Error processing slot range: %s, %s", slotRangeDir.getFileName(), e.getMessage()));
            e.printStackTrace();
            return CompletableFuture.failedFuture(e);
        }
    }

    private void processSlot(Path slotDir, CustomSequenceFileWriter entriesWriter,
            CustomSequenceFileWriter blocksWriter) throws IOException {
        Files.walkFileTree(slotDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (Files.isDirectory(file)) {
                    return FileVisitResult.CONTINUE;
                }

                String filePath = file.toString();
                String[] pathParts = filePath.split("/");
                String fileName = pathParts[pathParts.length - 1];
                String folderName = pathParts[pathParts.length - 2];
                String rowKeyWithoutExtension = fileName.contains(".")
                        ? fileName.substring(0, fileName.lastIndexOf('.'))
                        : fileName;

                if (!folderName.equals("blocks") && !folderName.equals("entries")) {
                    logger.warning("Skipping file: " + file
                            + ". There should be no other files in the slot directory other than blocks and entries.");
                    return FileVisitResult.CONTINUE;
                }

                byte[] fileContent = Files.readAllBytes(file);
                ImmutableBytesWritable key = new ImmutableBytesWritable(rowKeyWithoutExtension.getBytes());
                long timestamp = System.currentTimeMillis();

                String columnFamily = "x";
                String qualifier = "proto";

                @SuppressWarnings("deprecation")
                Cell cell = CellUtil.createCell(
                        rowKeyWithoutExtension.getBytes(),
                        Bytes.toBytes(columnFamily),
                        Bytes.toBytes(qualifier),
                        timestamp,
                        Cell.Type.Put.getCode(),
                        fileContent);

                Result result = Result.create(Collections.singletonList(cell));
                switch (folderName) {
                    case "blocks":
                        blocksWriter.append(key, result);
                        break;
                    case "entries":
                        entriesWriter.append(key, result);
                        break;
                    default:
                        logger.warning("Skipping file: " + file
                                + ". There should be no other files in the slot directory other than blocks and entries.");
                        break;
                }

                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void deleteDirectory(Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Override
    public void close() throws Exception {
        executorService.shutdown();
    }
}
