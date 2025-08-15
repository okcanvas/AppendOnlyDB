package aodb.codec;

import java.nio.ByteBuffer;

public final class VarInt {
    private VarInt(){}

    // ---- Read result ----
    public static final class Read {
        public final int  value;      // 32-bit convenience
        public final long valueLong;  // 64-bit full value
        public final int  size;

        public Read(int v, int s){
            this.value = v;
            this.valueLong = v & 0xFFFF_FFFFL;
            this.size = s;
        }
        public Read(long v, int s, boolean isLong){
            this.value = (int)v;      // truncate for int callers (as before)
            this.valueLong = v;
            this.size = s;
        }
    }

    // ---- int (unchanged) ----
    /** Number of bytes needed (1..5) for non-negative int */
    public static int len(int v){
        int n = 1;
        while ((v & ~0x7F) != 0) { v >>>= 7; n++; }
        return n;
    }

    /** Write non-negative int varint into buffer */
    public static void put(ByteBuffer buf, int v){
        while ((v & ~0x7F) != 0) { buf.put((byte)((v & 0x7F) | 0x80)); v >>>= 7; }
        buf.put((byte)v);
    }

    /** Read non-negative int varint from current position and advance */
    public static int readFrom(ByteBuffer bb){
        int v = 0, shift = 0, i = 0;
        while (i < 5) {
            byte b = bb.get();
            v |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return v;
            shift += 7; i++;
        }
        throw new IllegalStateException("VarInt too long");
    }

    /** Read non-negative int varint at absolute pos, without moving source buffer */
    public static Read getWithSize(ByteBuffer bb, int pos){
        int v = 0, shift = 0, i = 0;
        while (i < 5) {
            byte b = bb.get(pos + i);
            v |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return new Read(v, i + 1);
            shift += 7; i++;
        }
        throw new IllegalStateException("VarInt too long at pos=" + pos);
    }

    // ---- long (new) ----
    /** Number of bytes needed (1..10) for non-negative long */
    public static int lenLong(long v){
        int n = 1;
        while ((v & ~0x7FL) != 0L) { v >>>= 7; n++; }
        return n;
    }

    /** Write non-negative long varint into buffer */
    public static void putLong(ByteBuffer buf, long v){
        while ((v & ~0x7FL) != 0L) { buf.put((byte)((v & 0x7F) | 0x80)); v >>>= 7; }
        buf.put((byte)v);
    }

    /** Read non-negative long varint from current position and advance */
    public static long readLongFrom(ByteBuffer bb){
        long v = 0L; int shift = 0; int i = 0;
        while (i < 10) {
            byte b = bb.get();
            v |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return v;
            shift += 7; i++;
        }
        throw new IllegalStateException("VarLong too long");
    }

    /** Read non-negative long varint at absolute pos, without moving source buffer */
    public static Read getLongWithSize(ByteBuffer bb, int pos){
        long v = 0L; int shift = 0; int i = 0;
        while (i < 10) {
            byte b = bb.get(pos + i);
            v |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return new Read(v, i + 1, true);
            shift += 7; i++;
        }
        throw new IllegalStateException("VarLong too long at pos=" + pos);
    }
}
