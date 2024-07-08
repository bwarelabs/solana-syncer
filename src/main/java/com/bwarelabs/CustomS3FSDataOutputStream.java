package com.bwarelabs;

import org.apache.hadoop.fs.FSDataOutputStream;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class CustomS3FSDataOutputStream extends FSDataOutputStream {
    private static final Logger logger = Logger.getLogger(CustomS3FSDataOutputStream.class.getName());

    private final ByteArrayOutputStream buffer;
    private final String s3Key;
    private CompletableFuture<CompletedUpload> uploadFuture;

    public CustomS3FSDataOutputStream(Path slotRangeDir, String category) {
        this(new ByteArrayOutputStream(), slotRangeDir, category);
        logger.info("CustomS3FSDataOutputStream created for key: " + s3Key);
    }

    private CustomS3FSDataOutputStream(ByteArrayOutputStream buffer, Path slotRangeDir, String category) {
        super(buffer, null);
        this.buffer = buffer;
        this.s3Key = slotRangeDir.getFileName() + "/" + category + "/" + category + ".seq";
    }

    @Override
    public void close() throws IOException {
        logger.info("Closing stream for: " + s3Key);
        super.close();
        byte[] content = buffer.toByteArray();
        logger.info("Uploading " + s3Key + " to S3 asynchronously");

        try {
            uploadFuture = CosUtils.uploadToCos(s3Key, content);
        } catch (Exception e) {
            throw new IOException(String.format("Failed to upload %s to S3", s3Key), e);
        } finally {
            buffer.close();
        }
    }

    public CompletableFuture<CompletedUpload> getUploadFuture() {
        return uploadFuture;
    }

    public String getS3Key() {
        return s3Key;
    }
}
