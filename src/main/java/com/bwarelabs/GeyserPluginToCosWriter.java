package com.bwarelabs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.serializer.WritableSerialization;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * GeyserPluginToCosWriter class is responsible for reading data from local storage, processing it,
 * and uploading the processed data to Tencent Cloud Object Storage (COS) asynchronously.
 * <p>
 * The main steps performed by this class are:
 * 1. Identify slot range directories from the input storage path.
 * 2. For each slot range directory, process the subdirectories (slots).
 * 3. For each slot, create sequence files for different categories (entries, blocks, tx, tx_by_addr).
 * 4. Write the data to the sequence files.
 * 5. Upload the sequence files to COS asynchronously.
 * 6. Ensure all uploads are completed before finishing the process.
 * <p>
 * Asynchronous Processing:
 * - The processing of each slot range and the subsequent uploads are handled asynchronously.
 * - CompletableFuture is used to manage asynchronous tasks and ensure that all uploads are completed
 *   before the program exits.
 */
public class GeyserPluginToCosWriter {
    private static final Logger logger = Logger.getLogger(GeyserPluginToCosWriter.class.getName());

    public static void watchDirectory(Path path) {
        logger.info("Starting watch process...");

        try {
            // Process existing directories
            List<CompletableFuture<Void>> futures = Files.list(path)
                    .filter(Files::isDirectory)
                    .map(GeyserPluginToCosWriter::processSlotRange)
                    .collect(Collectors.toList());

            CompletableFuture<Void> initialUploads = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            initialUploads.thenRun(() -> logger.info("Initial slot ranges processed and uploaded."));

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

                        WatchEvent<Path> ev = (WatchEvent<Path>) event;
                        Path fileName = ev.context();
                        Path child = path.resolve(fileName);

                        if (Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)) {
                            CompletableFuture<Void> future = processSlotRange(child);
                            futures.add(future);
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

            CompletableFuture<Void> allUploads = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allUploads.thenRun(() -> logger.info("All slot ranges processed and uploaded."))
                    .join();

        } catch (Exception e) {
            logger.severe(String.format("Error processing directory: %s, %s", path, e.getMessage()));
        }
    }

    private static CompletableFuture<Void> processSlotRange(Path slotRangeDir) {
        logger.info("Processing slot range: " + slotRangeDir.getFileName());

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setStrings("io.serializations", WritableSerialization.class.getName(), ResultSerialization.class.getName());

        try (
                CustomS3FSDataOutputStream entriesStream = new CustomS3FSDataOutputStream(slotRangeDir, "entries");
                CustomSequenceFileWriter entriesWriter = new CustomSequenceFileWriter(hadoopConfig, entriesStream);

                CustomS3FSDataOutputStream blocksStream = new CustomS3FSDataOutputStream(slotRangeDir, "blocks");
                CustomSequenceFileWriter blocksWriter = new CustomSequenceFileWriter(hadoopConfig, blocksStream);

                CustomS3FSDataOutputStream txStream = new CustomS3FSDataOutputStream(slotRangeDir, "tx");
                CustomSequenceFileWriter txWriter = new CustomSequenceFileWriter(hadoopConfig, txStream);

                CustomS3FSDataOutputStream txByAddrStream = new CustomS3FSDataOutputStream(slotRangeDir, "tx_by_addr");
                CustomSequenceFileWriter txByAddrWriter = new CustomSequenceFileWriter(hadoopConfig, txByAddrStream);

                Stream<Path> slotDirs = Files.list(slotRangeDir)
        ) {
            slotDirs
                    .filter(Files::isDirectory)
                    .forEach(slotDir -> {
                        try {
                            processSlot(slotDir, entriesWriter, blocksWriter, txWriter, txByAddrWriter);
                        } catch (Exception e) {
                            logger.severe(String.format("Error processing slot: %s, %s", slotDir.getFileName(), e.getMessage()));                  }
                    });

            // Closing writers so the CustomS3FSDataOutputStream creates the futures
            entriesWriter.close();
            blocksWriter.close();
            txWriter.close();
            txByAddrWriter.close();

            return CompletableFuture.allOf(
                    entriesStream.getUploadFuture(),
                    blocksStream.getUploadFuture(),
                    txStream.getUploadFuture(),
                    txByAddrStream.getUploadFuture()
            ).thenRun(() -> {
                logger.info("Slot range processed: " + slotRangeDir.getFileName());
                try {
                    deleteDirectory(slotRangeDir);
                    logger.info("Deleted slot range: " + slotRangeDir.getFileName());
                } catch (Exception e) {
                    logger.severe(String.format("Error deleting slot range: %s, %s", slotRangeDir.getFileName(), e.getMessage()));
                }
            });
        } catch (Exception e) {
            logger.severe(String.format("Error processing slot range: %s, %s", slotRangeDir.getFileName(), e.getMessage()));
            return CompletableFuture.completedFuture(null);
        }
    }

    private static void processSlot(Path slotDir, CustomSequenceFileWriter entriesWriter, CustomSequenceFileWriter blocksWriter, CustomSequenceFileWriter txWriter, CustomSequenceFileWriter txByAddrWriter) throws IOException {
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
                String rowKeyWithoutExtension = fileName.contains(".") ?
                        fileName.substring(0, fileName.lastIndexOf('.')) :
                        fileName;

                if (folderName.equals("tx_by_addr")) {
                    rowKeyWithoutExtension = rowKeyWithoutExtension.replace("_", "/");
                }

                byte[] fileContent = Files.readAllBytes(file);
                ImmutableBytesWritable key = new ImmutableBytesWritable(rowKeyWithoutExtension.getBytes());
                long timestamp = System.currentTimeMillis();

                String columnFamily = "x";
                String qualifier;

                switch (folderName) {
                    case "entries":
                    case "blocks":
                    case "tx_by_addr":
                        qualifier = "proto";
                        break;
                    case "tx":
                        qualifier = "bin";
                        break;
                    default:
                        logger.warning("Unknown folder type: " + folderName);
                        return FileVisitResult.CONTINUE;
                }

                Cell cell = CellUtil.createCell(
                        rowKeyWithoutExtension.getBytes(),
                        Bytes.toBytes(columnFamily),
                        Bytes.toBytes(qualifier),
                        timestamp,
                        Cell.Type.Put.getCode(),
                        fileContent
                );

                Result result = Result.create(Collections.singletonList(cell));

                switch (folderName) {
                    case "entries":
                        entriesWriter.append(key, result);
                        break;
                    case "blocks":
                        blocksWriter.append(key, result);
                        break;
                    case "tx":
                        txWriter.append(key, result);
                        break;
                    case "tx_by_addr":
                        txByAddrWriter.append(key, result);
                        break;
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static void deleteDirectory(Path path) throws IOException {
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
}
