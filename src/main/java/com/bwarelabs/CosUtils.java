package com.bwarelabs;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.*;
import com.qcloud.cos.exception.*;
import com.qcloud.cos.model.*;
import com.qcloud.cos.internal.SkipMd5CheckStrategy;
import com.qcloud.cos.region.Region;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.qcloud.cos.transfer.*;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

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
        CONNECTION_TIMEOUT = Integer
                .parseInt(Utils.getRequiredProperty(properties, "cos-utils.tencent.connection-timeout"));
        THREAD_COUNT = Integer.parseInt(Utils.getRequiredProperty(properties, "bigtable.thread-count"));
        System.setProperty(SkipMd5CheckStrategy.DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY, "true");
    }

    static ExecutorService createUploadExecutorService() {
        return Executors.newFixedThreadPool(THREAD_COUNT);
    }

    public static final COSClient cosClient = createCOSClient();
    private static final ExecutorService uploadExecutorService = createUploadExecutorService();

    public static CompletableFuture<CompleteMultipartUploadResult> uploadToCos(final String key,
            InputStream inputStream) {
        return CompletableFuture.supplyAsync(() -> {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(BUCKET_NAME, key,
                    objectMetadata);
            InitiateMultipartUploadResult initiateResult = cosClient.initiateMultipartUpload(initiateRequest);
            final String uploadId = initiateResult.getUploadId();
            List<PartETag> partETags = new ArrayList<>();

            try {

                byte[] data = new byte[2 * BUFFER_SIZE];
                int bytesRead;
                int partNumber = 1;

                int offset = 0;
                while ((bytesRead = inputStream.read(data, offset, BUFFER_SIZE)) != -1) {
                    assert (bytesRead > 0);
                    offset += bytesRead;
                    if (offset >= BUFFER_SIZE) {
                        PartETag partETag = uploadPart(key, uploadId, partNumber++, data, offset, false);
                        partETags.add(partETag);
                        offset = 0;
                    }
                }

                logger.info(String.format("while loop completed. partNumber: %d", partNumber));

                // if we get another part too small error, this is probably the issue!!!!
                // can be fixed with 2 buffers, 1st one used to concatenate data from 2nd buffer
                // at the end of the loop
                if (offset > 0) {
                    // logger.info(String.format("buffer size is greater than 0. partNumber: %d",
                    // partNumber));
                    PartETag partETag = uploadPart(key, uploadId, partNumber++, data, offset, true);
                    partETags.add(partETag);
                }

                CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(BUCKET_NAME, key,
                        uploadId, partETags);
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

    private static PartETag uploadPart(String key, String uploadId, int partNumber, byte[] data, int size,
            boolean isLastPart) throws InterruptedException {
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

    public static void saveUploadedRangesToCos(String machineIdentifier, String tableName, String startRowKey, String endRowKey) {
        String checkpointKey = String.format("checkpoints/%s/%s/uploadedRanges.txt",tableName, machineIdentifier);
        String checkpointData = String.format("%s_%s\n", startRowKey, endRowKey);

        try {
            List<String> existingCheckpoints = loadUploadedRangesFromCos(tableName);

            existingCheckpoints.add(checkpointData);

            StringBuilder combinedData = new StringBuilder();
            for (String checkpoint : existingCheckpoints) {
                combinedData.append(checkpoint);
            }

            byte[] bytes = combinedData.toString().getBytes(StandardCharsets.UTF_8);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

            PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET_NAME, checkpointKey, inputStream, new ObjectMetadata());
            cosClient.putObject(putObjectRequest);

            logger.info("Checkpoint saved to COS for table " + tableName);
        } catch (Exception e) {
            logger.severe("Error saving checkpoint to COS: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static List<String> loadUploadedRangesFromCos(String tableName) throws IOException {
        String checkpointPrefix = String.format("checkpoints/%s/", tableName);
        List<String> checkpoints = new ArrayList<>();

        try {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(BUCKET_NAME)
                    .withPrefix(checkpointPrefix);

            ObjectListing objectListing;

            do {
                objectListing = cosClient.listObjects(listObjectsRequest);

                for (COSObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    String checkpointKey = objectSummary.getKey();

                    COSObject cosObject = cosClient.getObject(BUCKET_NAME, checkpointKey);
                    InputStream inputStream = cosObject.getObjectContent();

                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            checkpoints.add(line);
                        }
                    }
                }

                listObjectsRequest.setMarker(objectListing.getNextMarker());

            } while (objectListing.isTruncated());

        } catch (CosServiceException e) {
            logger.warning("No checkpoint data found for table " + tableName);
        } catch (Exception e) {
            logger.severe("Error reading checkpoint data from COS: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        return checkpoints;
    }
}
