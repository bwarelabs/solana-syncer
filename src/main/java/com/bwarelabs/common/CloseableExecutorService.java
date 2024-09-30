package com.bwarelabs.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class CloseableExecutorService implements AutoCloseable {
    private final ExecutorService executor;

    public CloseableExecutorService(ExecutorService executor) {
        this.executor = executor;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }
}
