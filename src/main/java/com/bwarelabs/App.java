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
    try (InputStream input = new FileInputStream("config.properties")) {
      properties.load(input);
    } catch (IOException ex) {
      logger.severe("Error loading configuration file: " + ex.getMessage());
      return;
    }

    String readSource = null;
    String blocksStartKey = null;
    String blocksLastKey = null;
    boolean useEmulator = false;

    for (String arg : args) {
      if (arg.startsWith("read-source=")) {
        readSource = arg.split("=")[1];
      } else if (arg.startsWith("start-key=")) {
        blocksStartKey = arg.split("=")[1];
      } else if (arg.startsWith("end-key=")) {
        blocksLastKey = arg.split("=")[1];
      } else if (arg.startsWith("use-emulator=")) {
        useEmulator = Boolean.parseBoolean(arg.split("=")[1]);
      }
    }

    if (readSource == null) {
      logger.severe("Error: 'read-source' argument is required. Valid values for 'read-source' are 'bigtable' and 'local-files'.");
      return;
    }

    if (readSource.equals("bigtable")) {
      try {
        BigTableToCosWriter bigTableToCosWriter = new BigTableToCosWriter(properties, blocksStartKey, blocksLastKey, useEmulator);
        bigTableToCosWriter.write();

        CosUtils.cosClient.shutdown();
        CosUtils.uploadExecutorService.shutdown();
      } catch (Exception e) {
        logger.severe(String.format("An error occurred while migrating data from BigTable: %s", e));
        e.printStackTrace();
      }
      logger.info("Done!");
      return;
    }

    if (readSource.equals("local-files")) {
      try {
        GeyserPluginToCosWriter.watchDirectory();
        logger.info("Done!");
      } catch (Exception e) {
        logger.severe(String.format("An error occurred while reading data from local files: %s", e));
      }
      return;
    }

    logger.severe("Error: Invalid 'read-source' argument. Valid values are 'bigtable' and 'local-files'.");
  }
}
