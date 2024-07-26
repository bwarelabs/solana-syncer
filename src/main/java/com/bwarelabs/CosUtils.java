package com.bwarelabs;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.BlockingInputStreamAsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.io.IOException;
import java.io.FileInputStream;

public class CosUtils {
    private static final Logger logger = Logger.getLogger(CosUtils.class.getName());

    private static final String BUCKET_NAME;
    private static final String COS_ENDPOINT;
    private static final String REGION;
    private static final String AWS_ID_KEY;
    private static final String AWS_SECRET_KEY;

    private static final ExecutorService executorService = Executors.newFixedThreadPool(200);

    static {
        Properties properties = new Properties();
        try (InputStream input = new FileInputStream("config.properties")) {
            properties.load(input);
        } catch (IOException ex) {
            logger.severe("Error loading configuration file: " + ex.getMessage());
            throw new RuntimeException("Error loading configuration file", ex);
        }

        BUCKET_NAME = Utils.getRequiredProperty(properties, "cos-utils.tencent.bucket-name");
        COS_ENDPOINT = Utils.getRequiredProperty(properties, "cos-utils.tencent.endpoint");
        REGION = Utils.getRequiredProperty(properties, "cos-utils.tencent.region");
        AWS_ID_KEY = Utils.getRequiredProperty(properties, "cos-utils.tencent.id-key");
        AWS_SECRET_KEY = Utils.getRequiredProperty(properties, "cos-utils.tencent.secret-key");
    }

    private static final S3AsyncClient s3AsyncClient = S3AsyncClient.crtBuilder()
            .endpointOverride(URI.create(COS_ENDPOINT))
            .region(Region.of(REGION))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(AWS_ID_KEY, AWS_SECRET_KEY)))
            .forcePathStyle(true)
            .build();

    private static final S3TransferManager transferManager = S3TransferManager.builder()
            .s3Client(s3AsyncClient)
            .build();

    public static CompletableFuture<CompletedUpload> uploadToCos(String key, InputStream inputStream) {
        if (key == null || key.trim().isEmpty()) {
            logger.severe("Key cannot be null or empty");
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        if (inputStream == null) {
            logger.severe("Input stream cannot be null");
            throw new IllegalArgumentException("Input stream cannot be null");
        }

        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(key)
                    .build();

            //BlockingInputStreamAsyncRequestBody body = AsyncRequestBody.forBlockingInputStream(null); // 'null' indicates a stream will be provided later.
            BlockingInputStreamAsyncRequestBody body = BlockingInputStreamAsyncRequestBody.builder().contentLength(null).subscribeTimeout(Duration.ofSeconds(120)).build();

            UploadRequest uploadRequest = UploadRequest.builder()
                    .putObjectRequest(putObjectRequest)
                    .requestBody(body)
                    .addTransferListener(LoggingTransferListener.create())
                    .build();


            logger.info(String.format("Starting upload for: %s", key));
            Upload upload = transferManager.upload(uploadRequest);
            logger.info(String.format("Started upload for: %s", key));

            body.writeInputStream(inputStream);

            return upload.completionFuture();

            //CompletableFuture<Void> writeFuture = CompletableFuture.runAsync(() -> {
            //    try {
            //        body.writeInputStream(inputStream);
            //    } catch (Exception e) {
            //        throw new RuntimeException("Failed to write input stream to body", e);
            //    }
            //}, executorService);

            //return writeFuture.thenCompose(v -> upload.completionFuture())
            //        .exceptionally(ex -> {
            //            throw new RuntimeException("Upload to COS failed", ex);
            //        });
        } catch (Exception e) {
            logger.severe("Input stream cannot be null");
            e.printStackTrace();
            throw new RuntimeException("Error initiating upload to COS", e);
        }
    }
}
