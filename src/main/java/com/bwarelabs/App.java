package com.bwarelabs;

import java.io.InputStream;
import java.nio.file.Path;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;
import java.io.FileInputStream;

public class App {
  private static final Logger logger = Logger.getLogger(App.class.getName());

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "/");

    Properties properties = new Properties();
    try (InputStream input = new FileInputStream("config.properties")) { // Specify the path to the external file
      properties.load(input);
    } catch (IOException ex) {
      logger.severe("Error loading configuration file: " + ex.getMessage());
      return;
    }

    String readSource = null;
    String blocksStartKey = null;
    String blocksLastKey = null;

    for (String arg : args) {
      if (arg.startsWith("read-source=")) {
        readSource = arg.split("=")[1];
      } else if (arg.startsWith("blocks-start-key=")) {
        blocksStartKey = arg.split("=")[1];
      } else if (arg.startsWith("blocks-last-key=")) {
        blocksLastKey = arg.split("=")[1];
      }
    }

    if (readSource == null) {
      logger.severe("Error: 'read-source' argument is required. Valid values for 'read-source' are 'bigtable' and 'local-files'.");
      return;
    }

    if (readSource.equals("bigtable")) {
      logger.info("Writing SequenceFiles from Bigtable tables: blocks, tx, tx-by-addr. For entries table, please use 'sync-entries' branch");

      try {
        BigTableToCosWriter bigTableToCosWriter = new BigTableToCosWriter(properties, blocksStartKey, blocksLastKey);
        bigTableToCosWriter.write();

        CosUtils.cosClient.shutdown();
        CosUtils.uploadExecutorService.shutdown();
      } catch (Exception e) {
        logger.severe(String.format("An error occurred while writing SequenceFiles from Bigtable tables: blocks, tx, tx-by-addr - %s", e));
        e.printStackTrace();
      }
      logger.info("Done!");
      return;
    }

    if (readSource.equals("local-files")) {
      String storagePath = Utils.getRequiredProperty(properties, "geyser-plugin.input-directory");
      logger.info("Reading data from local files from path " + storagePath);
      try {
        GeyserPluginToCosWriter.watchDirectory(Path.of(storagePath));
        logger.info("Done!");
      } catch (Exception e) {
        logger.severe(String.format("An error occurred while reading data from local files: %s", e));
      }
      return;
    }

    logger.severe("Error: Invalid 'read-source' argument. Valid values are 'bigtable' and 'local-files'.");
  }
}
