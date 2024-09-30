package com.bwarelabs.common;

import com.google.cloud.bigtable.data.v2.models.*;
import java.util.List;
import java.util.ArrayList;

public class BigtableBlock {
    private static final int PROTOBUF = 0;
    private static final int BINCODE = 1;
    private final long slot;
    private final int type;
    private final byte[] compressedBlock;

    public final List<BigtableCell> txs = new ArrayList<>();
    public final List<BigtableCell> txByAddr = new ArrayList<>();

    static {
        System.loadLibrary("solana_bigtable");
    }

    public BigtableBlock(Row row) {
        slot = Long.parseLong(row.getKey().toStringUtf8(), 16);

        RowCell rowCell = row.getCells().get(0);
        if (rowCell.getQualifier().toStringUtf8().equals("proto")) {
            this.type = PROTOBUF;
        } else {
            assert (rowCell.getQualifier().toStringUtf8().equals("bin"));
            this.type = BINCODE;
        }
        this.compressedBlock = rowCell.getValue().toByteArray();
        // Extract the transactions from the block (call JNI method)
        process();
    }

    public BigtableBlock(String key, byte[] qualifier, byte[] value) {
        this.slot = Long.parseLong(key, 16);
        if (qualifier[0] == 'p') { // proto
            this.type = PROTOBUF;
        } else { // bin
            assert (qualifier[0] == 'b');
            this.type = BINCODE;
        }
        this.compressedBlock = value;
        // Extract the transactions from the block (call JNI method)
        process();
    }

    /** JNI methods */
    public native void process();
}
