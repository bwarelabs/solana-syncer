package com.bwarelabs.geyser2cos;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import com.bwarelabs.common.CosUtils;

import picocli.CommandLine.Command;

@Command(name = "geyser-to-cos", mixinStandardHelpOptions = true, version = "0.1", description = "Exports data from Solana Geyser Plugin and loads it into COS.")
public class Starter implements Callable<Integer> {
    private static final Logger logger = Logger.getLogger(Starter.class.getName());

    @Override
    public Integer call() throws Exception {
        logger.info("Starting geyser-to-cos sync");

        Properties properties = new Properties();
        try (InputStream input = new FileInputStream("config.properties")) {
            properties.load(input);
        }
        try (CosUtils cosUtils = new CosUtils(properties);
                Writer geyserToCosWriter = new Writer(cosUtils, properties)) {
            geyserToCosWriter.watchDirectory();
        }
        return 0;
    }
}
