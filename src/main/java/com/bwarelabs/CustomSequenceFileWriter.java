package com.bwarelabs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.SequenceFile;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.adapters.read.RowAdapter;

import java.io.IOException;
import java.util.logging.Logger;

public class CustomSequenceFileWriter implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(CustomSequenceFileWriter.class.getName());

    private final SequenceFile.Writer writer;
    private final FSDataOutputStream fsDataOutputStream;
    private final RowAdapter rowAdapter = new RowAdapter();

    public CustomSequenceFileWriter(Configuration conf, FSDataOutputStream out) throws IOException {
        if (conf == null) {
            logger.severe("Configuration cannot be null");
            throw new IllegalArgumentException("Configuration cannot be null");
        }
        if (out == null) {
            logger.severe("FSDataOutputStream cannot be null");
            throw new IllegalArgumentException("FSDataOutputStream cannot be null");
        }
        this.fsDataOutputStream = out;
        this.writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.stream(out),
                SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
                SequenceFile.Writer.valueClass(Result.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
        logger.info("CustomSequenceFileWriter created successfully");
    }

    public void append(ImmutableBytesWritable key, Result value) throws IOException {
        if (key == null || value == null) {
            logger.severe("Key and value cannot be null");
            throw new IllegalArgumentException("Key and value cannot be null");
        }
        this.writer.append(key, value);
        // this.writer.flush();
    }

    public void append(ImmutableBytesWritable key, Row value) throws IOException {
        append(key, rowAdapter.adaptResponse(value));
    }


    @Override
    public void close() throws IOException {
        logger.info("Closing custom sequence file writer");
        this.writer.close();

        if (fsDataOutputStream != null) {
            fsDataOutputStream.close();
        }
    }
}
