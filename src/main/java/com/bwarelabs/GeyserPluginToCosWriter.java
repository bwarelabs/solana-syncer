package com.bwarelabs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.serializer.WritableSerialization;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
    private static final String SYNC_TYPE;
    private static final String STORAGE_PATH;

    private static final ExecutorService fixedThreadPool = Executors.newFixedThreadPool(8);

    static {
        Properties properties = new Properties();
        try (InputStream input = new FileInputStream("config.properties")) {
            properties.load(input);
        } catch (IOException ex) {
            logger.severe("Error loading configuration file: " + ex.getMessage());
            throw new RuntimeException("Error loading configuration file", ex);
        }

        SYNC_TYPE = Utils.getRequiredProperty(properties, "sync.type");
        STORAGE_PATH = Utils.getRequiredProperty(properties, "geyser-plugin.input-directory");
        logger.info("Reading data from local files from path " + STORAGE_PATH);
    }

    public static void watchDirectory() {
        Path path = Path.of(STORAGE_PATH);
        logger.info("Uploading existing directories...");

        Set<String> directoriesToSkip = new HashSet<>(Arrays.asList());

        try {
            // Process existing directories
            List<CompletableFuture<Void>> futures = Files.list(path)
                    .filter(Files::isDirectory)
                    .filter(dir -> !directoriesToSkip.contains(dir.getFileName().toString()))
                    .map(slotRangeDir -> CompletableFuture.supplyAsync(() -> processSlotRange(slotRangeDir), fixedThreadPool)
                        .thenComposeAsync(f -> f, fixedThreadPool))
                    .collect(Collectors.toList());

            CompletableFuture<Void> initialUploads = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            initialUploads.thenRun(() -> logger.info("Initial slot ranges processed and uploaded."));
            initialUploads.join();

            logger.info("Uploaded existing directories.");
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

                       WatchEvent<Path> ev = (WatchEvent<Path>) event;
                       Path fileName = ev.context();
                       Path child = path.resolve(fileName);

                       if (Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)) {
                           CompletableFuture<Void> future = CompletableFuture.runAsync(() -> processSlotRange(child), fixedThreadPool);
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
            e.printStackTrace();
        }
    }

    private static CompletableFuture<Void> processSlotRange(Path slotRangeDir) {
        logger.info("Processing slot range: " + slotRangeDir.getFileName());

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setStrings("io.serializations", WritableSerialization.class.getName(), ResultSerialization.class.getName());
        String range = String.valueOf(slotRangeDir.getFileName());

        try (
            CustomS3FSDataOutputStream blocksStream = new CustomS3FSDataOutputStream(range, "blocks", SYNC_TYPE);
            CustomSequenceFileWriter blocksWriter = new CustomSequenceFileWriter(hadoopConfig, blocksStream);

            CustomS3FSDataOutputStream entriesStream = new CustomS3FSDataOutputStream(range, "entries", SYNC_TYPE);
            CustomSequenceFileWriter entriesWriter = new CustomSequenceFileWriter(hadoopConfig, entriesStream);

            Stream<Path> slotDirs = Files.list(slotRangeDir)
        ) {
            slotDirs
                    .filter(Files::isDirectory)
                    .forEach(slotDir -> {
                        try {
                            processSlot(slotDir, entriesWriter, blocksWriter);
                        } catch (Exception e) {
                            logger.severe(String.format("Error processing slot: %s, %s", slotDir.getFileName(), e.getMessage()));
                            e.printStackTrace();
                        }
                    });

            // Closing writers so the CustomS3FSDataOutputStream creates the futures
            blocksWriter.close();

            return CompletableFuture.allOf(
                    blocksStream.getUploadFuture()
            ).thenRunAsync(() -> {
                logger.info("Slot range processed: " + slotRangeDir.getFileName() + ", deleting slot range...");
                try {
                   deleteDirectory(slotRangeDir);
                    logger.info("Deleted slot range: " + slotRangeDir.getFileName());
                } catch (Exception e) {
                    logger.severe(String.format("Error deleting slot range: %s, %s", slotRangeDir.getFileName(), e.getMessage()));
                }
            }, fixedThreadPool);
        } catch (Exception e) {
            logger.severe(String.format("Error processing slot range: %s, %s", slotRangeDir.getFileName(), e.getMessage()));
            e.printStackTrace();
            return CompletableFuture.completedFuture(null);
        }
    }

    private static void processSlot(Path slotDir, CustomSequenceFileWriter entriesWriter, CustomSequenceFileWriter blocksWriter) throws IOException {
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

                if (!folderName.equals("blocks") && !folderName.equals("entries")) {
                    logger.warning("Skipping file: " + file + ". There should be no other files in the slot directory other than blocks and entries.");
                    return FileVisitResult.CONTINUE;
                }

                byte[] fileContent = Files.readAllBytes(file);
                ImmutableBytesWritable key = new ImmutableBytesWritable(rowKeyWithoutExtension.getBytes());
                long timestamp = System.currentTimeMillis();

                String columnFamily = "x";
                String qualifier = "proto";

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
                    case "blocks":
                        blocksWriter.append(key, result);
                        break;
                    case "entries":
                        entriesWriter.append(key, result);
                        break;
                    default:
                        logger.warning("Skipping file: " + file + ". There should be no other files in the slot directory other than blocks and entries.");
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
