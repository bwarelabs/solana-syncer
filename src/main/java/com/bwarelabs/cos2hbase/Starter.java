package com.bwarelabs.cos2hbase;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import com.bwarelabs.common.CosUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "cos-to-hbase", mixinStandardHelpOptions = true, version = "0.1", description = "Exports data from COS and loads it into Hbase.")
public class Starter implements Callable<Integer> {
    private static final Logger logger = Logger.getLogger(Starter.class.getName());

    @Option(names = { "--start-key" }, description = "The start key to read data from.", required = true)
    private int startKey;

    @Option(names = { "--end-key" }, description = "The end key to read data to.", required = true)
    private int endKey;

    @Override
    public Integer call() throws Exception {
        logger.info("Starting cos-to-hbase sync");

        Properties properties = new Properties();
        try (InputStream input = new FileInputStream("config.properties")) {
            properties.load(input);
        }
        try (CosUtils cosUtils = new CosUtils(properties)) {
            Writer cosToHbaseWriter = new Writer(cosUtils, properties, startKey, endKey);
            cosToHbaseWriter.write();
        }

        logger.info("Finished cos-to-hbase sync");
        return 0;
    }
}
