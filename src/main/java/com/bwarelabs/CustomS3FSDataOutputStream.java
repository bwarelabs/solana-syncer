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
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(CustomS3FSDataOutputStream.class);

    private final PipedOutputStream pipedOutputStream;
    private final PipedInputStream pipedInputStream;
    private final String s3Key;
    private Upload uploadFuture;

    public CustomS3FSDataOutputStream(Path slotRangeDir, String category, String syncType) throws IOException {
        this(new PipedOutputStream(), slotRangeDir, category, syncType);
        logger.info("CustomS3FSDataOutputStream created for key: " + s3Key);
    }

    private CustomS3FSDataOutputStream(PipedOutputStream pipedOutputStream, Path slotRangeDir, String category, String syncType) throws IOException {
        super(pipedOutputStream, null);
        this.pipedOutputStream = pipedOutputStream;
        this.pipedInputStream = new PipedInputStream(pipedOutputStream);
        this.s3Key = syncType + "/" + category + "/" + slotRangeDir.getFileName() + "/" + category + ".seq";
        initiateUpload();
    }

    private void initiateUpload() {
        logger.info(String.format("Initiating upload for: %s", s3Key));
        try {
            uploadFuture = CosUtils.uploadToCos(s3Key, pipedInputStream);

            /*
            uploadFuture.exceptionally(ex -> {
                logger.severe(String.format("Failed to upload %s to S3", s3Key));
                ex.printStackTrace();
                return null;
            });
            */
        } catch (Exception e) {
            logger.severe(String.format("Failed to initiate upload %s to S3", s3Key));
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        logger.info("Closing stream for: " + s3Key);
        super.close();
        pipedOutputStream.close();
    }

    public Upload getUploadFuture() {
        return uploadFuture;
    }

    public String getS3Key() {
        return s3Key;
    }
}
