package com.bwarelabs.common;

import java.io.File;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.transfer.Download;
import com.qcloud.cos.transfer.TransferManager;
import java.util.concurrent.Executors;

public class CosDownloader implements AutoCloseable {
    private final TransferManager transferManager;
    private final String bucketName;

    public CosDownloader(COSClient cosClient, String bucketName, int threadCount) {
        this.bucketName = bucketName;
        transferManager = new TransferManager(cosClient, Executors.newFixedThreadPool(threadCount));
    }

    public void downloadByKey(String key, String localFilePath)
            throws Exception {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        Download download = transferManager.download(getObjectRequest, new File(localFilePath));
        download.waitForCompletion();
    }

    @Override
    public void close() throws Exception {
        transferManager.shutdownNow(false);
    }
}
