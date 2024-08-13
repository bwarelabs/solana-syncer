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
import com.qcloud.cos.internal.SkipMd5CheckStrategy;
import com.qcloud.cos.internal.crypto.*;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.http.HttpMethodName;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import com.qcloud.cos.utils.DateUtils;
import com.qcloud.cos.transfer.*;
import com.qcloud.cos.model.lifecycle.*;
import com.qcloud.cos.model.inventory.*;
import com.qcloud.cos.model.inventory.InventoryFrequency;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
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
    private static final int BUFFER_SIZE = 15 * 1024 * 1024; // 15MB
    
    private static final int MAX_RETRIES = 2;
    private static final int SOCKET_TIMEOUT;
    private static final int CONNECTION_TIMEOUT;
    private static final int THREAD_COUNT;

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
        // transferManagerConfiguration.setMultipartUploadThreshold(15 * 1024 * 1024);
        // transferManagerConfiguration.setMinimumUploadPartSize(0 * 1024 * 1024);
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
        clientConfig.setSocketTimeout(SOCKET_TIMEOUT * 1000);
        // Set the connection timeout period, which is 30s by default.
        clientConfig.setConnectionTimeout(CONNECTION_TIMEOUT * 1000);

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
        SOCKET_TIMEOUT = Integer.parseInt(Utils.getRequiredProperty(properties, "cos-utils.tencent.socket-timeout"));
        CONNECTION_TIMEOUT = Integer.parseInt(Utils.getRequiredProperty(properties, "cos-utils.tencent.connection-timeout"));
        THREAD_COUNT = Integer.parseInt(Utils.getRequiredProperty(properties, "bigtable.thread-count"));    
        System.setProperty(SkipMd5CheckStrategy.DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY, "true");
    }

    static ExecutorService createUploadExecutorService() {         
        return Executors.newFixedThreadPool(THREAD_COUNT);
    }

    public static final COSClient cosClient = createCOSClient();
    public static final TransferManager transferManager = createTransferManager(cosClient);
    private static final ExecutorService uploadExecutorService = createUploadExecutorService();

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

    public static CompletableFuture<CompleteMultipartUploadResult> uploadToCos(final String key, InputStream inputStream) {
        return CompletableFuture.supplyAsync(() -> {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(BUCKET_NAME, key, objectMetadata);
            InitiateMultipartUploadResult initiateResult = cosClient.initiateMultipartUpload(initiateRequest);
            final String uploadId = initiateResult.getUploadId();
            List<PartETag> partETags = new ArrayList<>();

            List<Thread> threads = new ArrayList<>();

            try {

                byte[] data = new byte[2 * BUFFER_SIZE];
                int bytesRead;
                int partNumber = 1;
                final Semaphore semaphore = new Semaphore(8);

                int offset = 0;
                while ((bytesRead = inputStream.read(data, offset, BUFFER_SIZE)) != -1) {
                    assert(bytesRead > 0);
                    offset += bytesRead;
                    if (offset >= BUFFER_SIZE) {
                        final int currentPartNumber = partNumber++;
                        final byte[] uploadData = Arrays.copyOf(data, offset);
                        semaphore.acquire();
                        threads.add(Thread.ofVirtual().start(() -> {
                            try {
                                PartETag partETag = uploadPart(key, uploadId, currentPartNumber, uploadData, uploadData.length, false);
                                partETags.add(partETag);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            } finally {
                                semaphore.release();
                            }
                        }));
                        offset = 0;
                    }
                }

                logger.info(String.format("while loop completed. partNumber: %d", partNumber));

                // if we get another part too small error, this is probably the issue!!!!
                // can be fixed with 2 buffers, 1st one used to concatenate data from 2nd buffer at the end of the loop
                if (offset > 0) {
                    // logger.info(String.format("buffer size is greater than 0. partNumber: %d", partNumber));
                    PartETag partETag = uploadPart(key, uploadId, partNumber++, data, offset, true);
                    partETags.add(partETag);
                }

                for (Thread thread : threads) {
                    thread.join();
                }

                // for (PartETag partETag : partETags) {
                //     logger.info(String.format("PartETag: %s", partETag.getETag()));
                // }

                CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(BUCKET_NAME, key, uploadId, partETags);
                return cosClient.completeMultipartUpload(completeRequest);

            } catch (IOException e) {
                logger.severe("Error reading from input stream: " + e.getMessage());
                e.printStackTrace();
                abortMultipartUpload(key, uploadId);
                throw new RuntimeException("Error reading from input stream", e);
            } catch (CosClientException e) {
                logger.severe("COS Client Exception: " + e.getMessage());
                e.printStackTrace();
                abortMultipartUpload(key, uploadId);
                throw e;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, uploadExecutorService);
    }

    private static PartETag uploadPart(String key, String uploadId, int partNumber, byte[] data, int size, boolean isLastPart) throws InterruptedException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data, 0, size);
        UploadPartRequest uploadPartRequest = new UploadPartRequest()
                .withBucketName(BUCKET_NAME)
                .withKey(key)
                .withUploadId(uploadId)
                .withPartNumber(partNumber)
                .withInputStream(byteArrayInputStream)
                .withPartSize(size)
                .withLastPart(isLastPart);

        int attempts = 0;
        while (true) {
            try {
                UploadPartResult uploadPartResult = cosClient.uploadPart(uploadPartRequest);
                if (attempts > 0) {
                    logger.info("Successfully uploaded part number " + partNumber + " after " + attempts + " attempts");
                }
                return uploadPartResult.getPartETag();
            } catch (CosClientException e) {
                attempts++;
                if (attempts >= MAX_RETRIES) {
                    throw e;
                }
                logger.warning("Error uploading part number " + partNumber + ": " + e + ". Retrying...");
                e.printStackTrace();
                Thread.sleep(2000); 
            }
        }
    }

    private static void abortMultipartUpload(String key, String uploadId) {
        try {
            cosClient.abortMultipartUpload(new AbortMultipartUploadRequest(BUCKET_NAME, key, uploadId));
        } catch (Exception e) {
            logger.severe("Error aborting multipart upload: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
