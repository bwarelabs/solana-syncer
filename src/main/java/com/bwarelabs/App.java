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
    String bigtableTable = null;
    String[] validBigtableTables = {"blocks", "entries", "tx", "tx-by-addr"};

    for (String arg : args) {
      if (arg.startsWith("read-source=")) {
        readSource = arg.split("=")[1];
      } else if (readSource != null && readSource.equals("bigtable") && bigtableTable == null) {
        bigtableTable = arg;
      }
    }

    if (readSource == null) {
      logger.severe("Error: 'read-source' argument is required. Valid values for 'read-source' are 'bigtable' and 'local-files'.");
      return;
    }

    if (readSource.equals("bigtable")) {
      if (bigtableTable == null || !Arrays.asList(validBigtableTables).contains(bigtableTable)) {
        logger.severe("Error: When 'read-source' is 'bigtable', a second argument must be provided with one of the following values: 'blocks', 'entries', 'tx', 'tx-by-addr'.");
        return;
      }
      logger.info("Writing SequenceFiles from Bigtable table: " + bigtableTable);
      try {
        BigTableToCosWriter bigTableToCosWriter = new BigTableToCosWriter(properties);
        bigTableToCosWriter.write(bigtableTable);
        logger.info("Done!");
        CosUtils.cosClient.shutdown();
      } catch (Exception e) {
        logger.severe(String.format("An error occurred while writing SequenceFiles from Bigtable table: %s - %s", bigtableTable, e));
      }
      return;
    }

    if (readSource.equals("local-files")) {
      String storagePath = Utils.getRequiredProperty(properties,"geyser-plugin.input-directory");
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
