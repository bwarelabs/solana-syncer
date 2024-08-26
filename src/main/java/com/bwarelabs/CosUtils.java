package com.bwarelabs;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.*;
import com.qcloud.cos.exception.*;
import com.qcloud.cos.model.*;
import com.qcloud.cos.internal.SkipMd5CheckStrategy;
import com.qcloud.cos.region.Region;

import java.io.*;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class CosUtils {
    private static final Logger logger = Logger.getLogger(CosUtils.class.getName());

    private static final String BUCKET_NAME;
    private static final String REGION;
    private static final String COS_ID_KEY;
    private static final String COS_SECRET_KEY;
    private static final String SYNC_TYPE; 
    private static final int BUFFER_SIZE = 60 * 1024 * 1024; // 60MB

    private static final int SOCKET_TIMEOUT;
    private static final int CONNECTION_TIMEOUT;
    private static final int THREAD_COUNT;

    static COSClient createCOSClient() {
        // Set the user identity information.
        // Log in to the [CAM console](https://console.cloud.tencent.com/cam/capi) to
        // view and manage the `SECRETID` and `SECRETKEY` of your project.
        String secretId = COS_ID_KEY;
        String secretKey = COS_SECRET_KEY;
        COSCredentials cred = new BasicCOSCredentials(secretId, secretKey);

        // `ClientConfig` contains the COS client configuration for subsequent COS
        // requests.
        ClientConfig clientConfig = new ClientConfig();

        // Set the bucket region.
        // For more information on COS regions, visit
        // https://cloud.tencent.com/document/product/436/6224.
        clientConfig.setRegion(new Region(REGION));

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
        REGION = Utils.getRequiredProperty(properties, "cos-utils.tencent.region");
        COS_ID_KEY = Utils.getRequiredProperty(properties, "cos-utils.tencent.id-key");
        COS_SECRET_KEY = Utils.getRequiredProperty(properties, "cos-utils.tencent.secret-key");
        SOCKET_TIMEOUT = Integer.parseInt(Utils.getRequiredProperty(properties, "cos-utils.tencent.socket-timeout"));
        CONNECTION_TIMEOUT = Integer
                .parseInt(Utils.getRequiredProperty(properties, "cos-utils.tencent.connection-timeout"));
        THREAD_COUNT = Integer.parseInt(Utils.getRequiredProperty(properties, "bigtable.thread-count"));
        SYNC_TYPE = Utils.getRequiredProperty(properties, "sync.type");
        System.setProperty(SkipMd5CheckStrategy.DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY, "true");
    }

    static ExecutorService createUploadExecutorService() {
        return Executors.newFixedThreadPool(3 * THREAD_COUNT);
    }

    public static final COSClient cosClient = createCOSClient();
    public static final ExecutorService uploadExecutorService = createUploadExecutorService();

    public static CompletableFuture<CompleteMultipartUploadResult> uploadToCos(final String key,
            InputStream inputStream, CustomS3FSDataOutputStream outputStream) {
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

                if (offset > 0) {
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
                abortMultipartUpload(key, uploadId);

                Throwable cause = e.getCause();
                boolean skipError = false;

                if (cause instanceof SocketException && cause.getMessage().contains("Connection timed out")) {
                    logger.info("Connection timed out.");
                    skipError = true;
                } else {
                    while (cause != null && !skipError) {
                        if (cause instanceof SocketException && cause.getMessage().contains("Connection timed out")) {
                            logger.info("Connection timed out  from while loop.");
                            skipError = true;
                        } else if (cause instanceof CosServiceException serviceException) {
                            if ("ServiceUnavailable".equals(serviceException.getErrorCode()) && serviceException.getStatusCode() == 503) {
                                logger.info("Service unavailable (503) error detected.");
                                skipError = true;
                            }
                        }
                        cause = cause.getCause();
                    }
                }

                if (skipError) {
                    outputStream.setControlledClose(true);
                    try {
                        outputStream.close();
                        inputStream.close();
                    } catch (IOException e1) {
                        logger.severe("Error closing output stream: " + e1.getMessage());
                        e1.printStackTrace();
                    }

                    return null;
                } else {
                    logger.severe("Error was not a connection timeout nor a service unavailable error.");
                    throw e;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, uploadExecutorService);
    }

    private static PartETag uploadPart(String key, String uploadId, int partNumber, byte[] data, int size,
            boolean isLastPart) throws InterruptedException, CosClientException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data, 0, size);
        UploadPartRequest uploadPartRequest = new UploadPartRequest()
                .withBucketName(BUCKET_NAME)
                .withKey(key)
                .withUploadId(uploadId)
                .withPartNumber(partNumber)
                .withInputStream(byteArrayInputStream)
                .withPartSize(size)
                .withLastPart(isLastPart);


        UploadPartResult uploadPartResult = cosClient.uploadPart(uploadPartRequest);

        return uploadPartResult.getPartETag();
    }

    private static void abortMultipartUpload(String key, String uploadId) {
        try {
            cosClient.abortMultipartUpload(new AbortMultipartUploadRequest(BUCKET_NAME, key, uploadId));
        } catch (Exception e) {
            logger.severe("Error aborting multipart upload: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static String getCheckpointPrefix(String tableName) {
        return String.format("%s/checkpoints/%s", SYNC_TYPE, tableName);
    }

    public static void saveUploadedRangesToCos(String tableName, String startRowKey, String endRowKey) {
        String rangeFileName = String.format("%s/%s_%s.txt", getCheckpointPrefix(tableName), startRowKey, endRowKey);
        String fileContent = String.format("%s_%s\n", startRowKey, endRowKey);

        try {
            byte[] bytes = fileContent.getBytes(StandardCharsets.UTF_8);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(bytes.length);

            PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET_NAME, rangeFileName, inputStream, metadata);
            cosClient.putObject(putObjectRequest);

            logger.info("Range file saved to COS for table " + tableName + ": " + rangeFileName);
        } catch (Exception e) {
            logger.severe("Error saving range file to COS: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static List<String> loadUploadedRangesFromCos(String tableName) throws IOException {
        String checkpointPrefix = getCheckpointPrefix(tableName);
        List<String> uploadedRanges = new ArrayList<>();

        try {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(BUCKET_NAME)
                    .withPrefix(checkpointPrefix);

            ObjectListing objectListing;

            do {
                objectListing = cosClient.listObjects(listObjectsRequest);

                for (COSObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    String checkpointKey = objectSummary.getKey();
                    String fileName = checkpointKey.substring(checkpointKey.lastIndexOf('/') + 1);

                    if (checkpointKey.contains("/failed/")) {
                        continue;
                    }

                    if (fileName.endsWith(".txt")) {
                        String range = fileName.replace(".txt", "");
                        uploadedRanges.add(range);
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

        return uploadedRanges;
    }

    public static void saveFailedRangesToCos(String tableName, String startRowKey, String endRowKey) {
        String rangeFileName = String.format("%s/failed/%s_%s.txt", getCheckpointPrefix(tableName), startRowKey, endRowKey);
        String fileContent = String.format("%s_%s\n", startRowKey, endRowKey);

        try {
            byte[] bytes = fileContent.getBytes(StandardCharsets.UTF_8);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(bytes.length);

            PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET_NAME, rangeFileName, inputStream, metadata);
            cosClient.putObject(putObjectRequest);

            logger.info("Failed range file saved to COS for table " + tableName + ": " + rangeFileName);
        } catch (Exception e) {
            logger.severe("Error saving range file to COS: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
