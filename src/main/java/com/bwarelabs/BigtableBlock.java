package com.bwarelabs;

import com.google.cloud.bigtable.data.v2.models.*;
import java.util.List;
import java.util.ArrayList;
import com.bwarelabs.BigtableCell;

public class BigtableBlock {
    private static final int PROTOBUF = 0;
    private static final int BINCODE = 1;
    private final long slot;
    private final int type;
    private final byte[] compressedBlock;

    public final List<BigtableCell> txs;
    public final List<BigtableCell> txByAddrs;

    static {
        System.loadLibrary("solana_bigtable");
    }

    public BigtableBlock(Row row) {
        txs = new ArrayList<>();
        txByAddrs = new ArrayList<>();
        slot = Long.parseLong(row.getKey().toStringUtf8(), 16);

        RowCell rowCell = row.getCells().get(0);
        if (rowCell.getQualifier().toStringUtf8().equals("proto")) {
            this.type = PROTOBUF;
        } else {
            assert (rowCell.getQualifier().toStringUtf8().equals("bin"));
            this.type = BINCODE;
        }
        this.compressedBlock = rowCell.getValue().toByteArray();
    }

    public native void process();
}
