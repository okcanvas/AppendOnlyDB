// FILE: src/main/java/aodb/hash/Fingerprint64.java
package aodb.hash;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Minimal xxHash64 (pure Java). Good avalanche & speed; used for key fingerprinting. */
public final class Fingerprint64 {
    private static final long P1 = 0x9E3779B185EBCA87L;
    private static final long P2 = 0xC2B2AE3D27D4EB4FL;
    private static final long P3 = 0x165667B19E3779F9L;
    private static final long P4 = 0x85EBCA77C2B2AE63L;
    private static final long P5 = 0x27D4EB2F165667C5L;

    private Fingerprint64() {}

    public static long of(byte[] data) { return of(data, 0, data.length, 0); }
    public static long of(String s) { return of(s.getBytes(StandardCharsets.UTF_8)); }

    public static long of(ByteBuffer buf, int off, int len) {
        if (buf.hasArray()) return of(buf.array(), buf.arrayOffset() + off, len, 0);
        byte[] tmp = new byte[len];
        int p = buf.position();
        buf.position(off);
        buf.get(tmp);
        buf.position(p);
        return of(tmp, 0, len, 0);
    }

    private static long of(byte[] data, int off, int len, long seed) {
        int end = off + len;
        long h;
        if (len >= 32) {
            long v1 = seed + P1 + P2;
            long v2 = seed + P2;
            long v3 = seed + 0;
            long v4 = seed - P1;
            int i = off;
            int limit = end - 32;
            while (i <= limit) {
                v1 = round(v1, readLong(data, i)); i += 8;
                v2 = round(v2, readLong(data, i)); i += 8;
                v3 = round(v3, readLong(data, i)); i += 8;
                v4 = round(v4, readLong(data, i)); i += 8;
            }
            h = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7) +
                Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);
            h = mergeRound(h, v1);
            h = mergeRound(h, v2);
            h = mergeRound(h, v3);
            h = mergeRound(h, v4);
            off = i;
        } else {
            h = seed + P5;
        }
        h += len;
        while (off + 8 <= end) {
            long k1 = readLong(data, off); off += 8;
            h ^= round(0, k1);
            h = Long.rotateLeft(h, 27) * P1 + P4;
        }
        if (off + 4 <= end) {
            h ^= (readInt(data, off) & 0xFFFFFFFFL) * P1;
            h = Long.rotateLeft(h, 23) * P2 + P3;
            off += 4;
        }
        while (off < end) {
            h ^= (data[off++] & 0xFF) * P5;
            h = Long.rotateLeft(h, 11) * P1;
        }
        h ^= h >>> 33; h *= P2;
        h ^= h >>> 29; h *= P3;
        h ^= h >>> 32;
        return h;
    }

    private static long round(long acc, long input) {
        acc += input * P2;
        acc = Long.rotateLeft(acc, 31);
        acc *= P1;
        return acc;
    }
    private static long mergeRound(long acc, long val) {
        val = round(0, val);
        acc ^= val;
        acc = acc * P1 + P4;
        return acc;
    }
    private static long readLong(byte[] b, int i) {
        return ((long)b[i] & 0xFF)       |
               (((long)b[i+1] & 0xFF) << 8)  |
               (((long)b[i+2] & 0xFF) << 16) |
               (((long)b[i+3] & 0xFF) << 24) |
               (((long)b[i+4] & 0xFF) << 32) |
               (((long)b[i+5] & 0xFF) << 40) |
               (((long)b[i+6] & 0xFF) << 48) |
               (((long)b[i+7] & 0xFF) << 56);
    }
    private static int readInt(byte[] b, int i) {
        return (b[i] & 0xFF) |
               ((b[i+1] & 0xFF) << 8) |
               ((b[i+2] & 0xFF) << 16) |
               ((b[i+3] & 0xFF) << 24);
    }
}
