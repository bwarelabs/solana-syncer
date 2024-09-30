package com.bwarelabs.bigtable2cos;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import com.bwarelabs.common.CosUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "bigtable-to-cos", mixinStandardHelpOptions = true, version = "0.1", description = "Exports data from Google BigTable and loads it into COS.")
public class Starter implements Callable<Integer> {
    private static final Logger logger = Logger.getLogger(Starter.class.getName());

    @Option(names = { "--start-key" }, description = "The start key to read data from.", required = true)
    private String startKey;

    @Option(names = { "--end-key" }, description = "The end key to read data to.", required = true)
    private String endKey;

    @Option(names = { "--use-emulator" }, description = "Whether to use the BigTable emulator.")
    private boolean useEmulator = false;

    @Override
    public Integer call() throws Exception {
        logger.info("Starting bigtable-to-cos sync");

        Properties properties = new Properties();
        try (InputStream input = new FileInputStream("config.properties")) {
            properties.load(input);
        }
        try (CosUtils cosUtils = new CosUtils(properties);
                Writer bigTableToCosWriter = new Writer(cosUtils, properties, startKey, endKey, useEmulator)) {
            bigTableToCosWriter.write();
        }
        return 0;
    }
}
