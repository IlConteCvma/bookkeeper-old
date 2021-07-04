package org.apache.bookkeeper.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class MyEntryLoggerUtils {


    public static ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = generateDataString(ledger, entry).getBytes();
        //metadata entry setup
        ByteBuf bb = Unpooled.buffer(Long.BYTES + Long.BYTES + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }

    public static String generateDataString(long ledger, long entry) {
        return ("ledger-" + ledger + "-" + entry);
    }
}
