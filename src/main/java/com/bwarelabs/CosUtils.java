package com.bwarelabs;

/*
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
*/
import com.qcloud.cos.COSClient;
import com.qcloud.cos.COSEncryptionClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.*;
import com.qcloud.cos.exception.*;
import com.qcloud.cos.model.*;
import com.qcloud.cos.internal.crypto.*;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.http.HttpMethodName;
import com.qcloud.cos.utils.DateUtils;
import com.qcloud.cos.transfer.*;
import com.qcloud.cos.model.lifecycle.*;
import com.qcloud.cos.model.inventory.*;
import com.qcloud.cos.model.inventory.InventoryFrequency;

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

    static TransferManager createTransferManager(COSClient cosClient) {
        // Set the thread pool size. We recommend you set the size of your thread pool
        // to 16 or 32 to maximize network resource utilization, provided your client
        // and COS networks are sufficient (for example, uploading a file to a COS
        // bucket from a CVM instance in the same region).
        // We recommend you use a smaller value to avoid timeout caused by slow network
        // speed if you are transferring data over a public network with poor bandwidth
        // quality.
        ExecutorService threadPool = Executors.newFixedThreadPool(32);

        // Pass a `threadpool`; otherwise, a single-thread pool will be generated in
        // `TransferManager` by default.
        TransferManager transferManager = new TransferManager(cosClient, threadPool);

        // Set the configuration items of the advanced API.
        // Set the threshold and part size for multipart upload to 5 MB and 1 MB
        // respectively.
        TransferManagerConfiguration transferManagerConfiguration = new TransferManagerConfiguration();
        transferManagerConfiguration.setMultipartUploadThreshold(5 * 1024 * 1024);
        transferManagerConfiguration.setMinimumUploadPartSize(1 * 1024 * 1024);
        transferManager.setConfiguration(transferManagerConfiguration);

        return transferManager;
    }

    static COSClient createCOSClient() {
        // Set the user identity information.
        // Log in to the [CAM console](https://console.cloud.tencent.com/cam/capi) to
        // view and manage the `SECRETID` and `SECRETKEY` of your project.
        String secretId = AWS_ID_KEY;
        String secretKey = AWS_SECRET_KEY;
        COSCredentials cred = new BasicCOSCredentials(secretId, secretKey);

        // `ClientConfig` contains the COS client configuration for subsequent COS
        // requests.
        ClientConfig clientConfig = new ClientConfig();

        // Set the bucket region.
        // For more information on COS regions, visit
        // https://cloud.tencent.com/document/product/436/6224.
        clientConfig.setRegion(new Region(REGION));

        // Set the request protocol to `http` or `https`.
        // For v5.6.53 or earlier, HTTPS is recommended.
        // For v5.6.54 or later, HTTPS is used by default.
        // clientConfig.setHttpProtocol(HttpProtocol.https);

        // The following settings are optional:

        // Set the read timeout period, which is 30s by default.
        clientConfig.setSocketTimeout(30 * 1000);
        // Set the connection timeout period, which is 30s by default.
        clientConfig.setConnectionTimeout(30 * 1000);

        // Generate a COS client.
        return new COSClient(cred, clientConfig);
    }

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

    public static final COSClient cosClient = createCOSClient();
    public static final TransferManager transferManager = createTransferManager(cosClient);

    /*
     * private static final S3AsyncClient s3AsyncClient = S3AsyncClient.crtBuilder()
     * .endpointOverride(URI.create(COS_ENDPOINT))
     * .region(Region.of(REGION))
     * .credentialsProvider(
     * StaticCredentialsProvider.create(AwsBasicCredentials.create(AWS_ID_KEY,
     * AWS_SECRET_KEY)))
     * .forcePathStyle(COS_ENDPOINT.contains("http://localhost:9000"))
     * .build();
     * 
     * private static final S3TransferManager transferManager =
     * S3TransferManager.builder()
     * .s3Client(s3AsyncClient)
     * .build();
     */

    /*
     * public static CompletableFuture<CompletedUpload> uploadToCos(String key,
     * InputStream inputStream) {
     * if (key == null || key.trim().isEmpty()) {
     * logger.severe("Key cannot be null or empty");
     * throw new IllegalArgumentException("Key cannot be null or empty");
     * }
     * if (inputStream == null) {
     * logger.severe("Input stream cannot be null");
     * throw new IllegalArgumentException("Input stream cannot be null");
     * }
     * 
     * try {
     * PutObjectRequest putObjectRequest = PutObjectRequest.builder()
     * .bucket(BUCKET_NAME)
     * .key(key)
     * .build();
     * 
     * // BlockingInputStreamAsyncRequestBody body =
     * // AsyncRequestBody.forBlockingInputStream(null); // 'null' indicates a
     * stream
     * // will be provided later.
     * BlockingInputStreamAsyncRequestBody body =
     * BlockingInputStreamAsyncRequestBody.builder().contentLength(null)
     * .subscribeTimeout(Duration.ofSeconds(120)).build();
     * 
     * UploadRequest uploadRequest = UploadRequest.builder()
     * .putObjectRequest(putObjectRequest)
     * .requestBody(body)
     * .addTransferListener(LoggingTransferListener.create())
     * .build();
     * 
     * logger.info(String.format("Starting upload for: %s", key));
     * Upload upload = transferManager.upload(uploadRequest);
     * logger.info(String.format("Started upload for: %s", key));
     * 
     * body.writeInputStream(inputStream);
     * logger.info(String.format("Wrote input stream for: %s", key));
     * 
     * return upload.completionFuture();
     * 
     * // CompletableFuture<Void> writeFuture = CompletableFuture.runAsync(() -> {
     * // try {
     * // body.writeInputStream(inputStream);
     * // } catch (Exception e) {
     * // throw new RuntimeException("Failed to write input stream to body", e);
     * // }
     * // }, executorService);
     * 
     * // return writeFuture.thenCompose(v -> upload.completionFuture())
     * // .exceptionally(ex -> {
     * // throw new RuntimeException("Upload to COS failed", ex);
     * // });
     * } catch (Exception e) {
     * logger.severe("Input stream cannot be null");
     * e.printStackTrace();
     * throw new RuntimeException("Error initiating upload to COS", e);
     * }
     * }
     */

    public static Upload uploadToCos(String key, InputStream inputStream) {
        ObjectMetadata objectMetadata = new ObjectMetadata();

        PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET_NAME, key, inputStream, objectMetadata);

        Upload upload = null;

        try {
            // The advanced API will return an async result `Upload`.
            // You can synchronously call the `waitForUploadResult` method to wait for the
            // upload to complete. If the upload is successful, `UploadResult` will be
            // returned; otherwise, an exception will be reported.
            upload = transferManager.upload(putObjectRequest);
        } catch (CosServiceException e) {
            e.printStackTrace();
        } catch (CosClientException e) {
            e.printStackTrace();
        }

        return upload;
    }
}
