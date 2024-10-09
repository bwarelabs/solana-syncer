package com.bwarelabs;

import java.io.IOException;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.bwarelabs.cos2hbase.Writer;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "syncer", version = "0.1", mixinStandardHelpOptions = true, description = "Solana data synchronizer application.", subcommands = {
    com.bwarelabs.cos2hbase.Starter.class, com.bwarelabs.bigtable2cos.Starter.class,
    com.bwarelabs.geyser2cos.Starter.class })
public class Main implements Runnable {
  private static final Logger logger = Logger.getLogger(Main.class.getName());

  public static void main(String[] args) throws SecurityException, IOException {
    LogManager.getLogManager().readConfiguration(
        Writer.class.getClassLoader().getResourceAsStream("logging.properties"));
    System.exit(new CommandLine(new Main()).execute(args));
  }

  @Override
  public void run() {
    logger.info("Please specify a subcommand. Use --help for usage info.");
  }
}
