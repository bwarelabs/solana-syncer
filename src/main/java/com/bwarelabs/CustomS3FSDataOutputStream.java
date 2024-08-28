package com.bwarelabs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.LoggerFactory;
import com.qcloud.cos.model.*;
import com.qcloud.cos.transfer.*;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class CustomS3FSDataOutputStream extends FSDataOutputStream {
    private static final Logger logger = Logger.getLogger(CustomS3FSDataOutputStream.class.getName());

    private final PipedOutputStream pipedOutputStream;
    private final PipedInputStream pipedInputStream;
    private final String cosPath;
    private CompletableFuture uploadFuture;
    private boolean controlledClose = false;

    public CustomS3FSDataOutputStream(String range, String tableName, String syncType) throws IOException {
        this(new PipedOutputStream(), range, tableName, syncType);
        logger.info("CustomS3FSDataOutputStream created for key: " + cosPath);
    }

    private CustomS3FSDataOutputStream(PipedOutputStream pipedOutputStream, String range, String tableName, String syncType) throws IOException {
        super(pipedOutputStream, null);
        this.pipedOutputStream = pipedOutputStream;
        this.pipedInputStream = new PipedInputStream(pipedOutputStream, 30 * 1024 * 1024);
        this.cosPath = syncType + "/" + tableName + "/" + range + "/" + tableName + ".seq";
        initiateUpload();
    }

    private void initiateUpload() {
        logger.info(String.format("Initiating upload for: %s", cosPath));
        try {
            uploadFuture = CosUtils.uploadToCos(cosPath, pipedInputStream, this);
        } catch (Exception e) {
            logger.severe(String.format("Failed to initiate upload %s to S3", cosPath));
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        logger.info("Closing stream for: " + cosPath);
        super.close();
        pipedOutputStream.close();
    }

    public CompletableFuture<Void> getUploadFuture() {
        return uploadFuture;
    }

    public void setControlledClose(boolean controlledClose) {
        logger.info("Setting controlledClose to: " + controlledClose);
        this.controlledClose = controlledClose;
    }

    public boolean isControlledClose() {
        return controlledClose;
    }
}
