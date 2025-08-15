package aodb.hash;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;

/** CRC helper.
 *  -Daodb.noCrc=true       => return 0, skip compute/verify (bench only)
 *  -Daodb.crc32c=false     => force CRC32 instead of CRC32C
 */
public final class Checksums {
    private static final boolean NO_CRC = Boolean.getBoolean("aodb.noCrc");
    private static final boolean USE_CRC32C = !Boolean.getBoolean("aodb.crc32c.false");

    private Checksums(){}

    /** Compute CRC over [start, end) from a buffer whose position may differ */
    public static int compute(ByteBuffer src, int start, int endExclusive){
        if (NO_CRC) return 0;
        int len = Math.max(0, endExclusive - start);
        if (len == 0) return 0;
        ByteBuffer dup = src.duplicate();
        dup.position(start).limit(endExclusive);
        return compute(dup);
    }

    /** Compute CRC over the remaining bytes of the buffer */
    public static int compute(ByteBuffer slice){
        if (NO_CRC) return 0;
        if (USE_CRC32C) {
            CRC32C c = new CRC32C();
            if (slice.hasArray()) {
                c.update(slice.array(), slice.arrayOffset() + slice.position(), slice.remaining());
            } else {
                while (slice.hasRemaining()) {
                    int n = Math.min(slice.remaining(), 1 << 14);
                    byte[] tmp = new byte[n];
                    int oldPos = slice.position();
                    slice.get(tmp);
                    c.update(tmp, 0, n);
                }
            }
            return (int)c.getValue();
        } else {
            CRC32 c = new CRC32();
            if (slice.hasArray()) {
                c.update(slice.array(), slice.arrayOffset() + slice.position(), slice.remaining());
            } else {
                while (slice.hasRemaining()) {
                    int n = Math.min(slice.remaining(), 1 << 14);
                    byte[] tmp = new byte[n];
                    slice.get(tmp);
                    c.update(tmp, 0, n);
                }
            }
            return (int)c.getValue();
        }
    }
}