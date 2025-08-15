package aodb.storage;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

public final class MMapUnmapper {
    private MMapUnmapper(){}
    public static void unmap(MappedByteBuffer mbb){
        if (mbb == null) return;
        try {
            Method m = mbb.getClass().getMethod("cleaner");
            m.setAccessible(true);
            Object cleaner = m.invoke(mbb);
            if (cleaner != null) {
                Method clean = cleaner.getClass().getMethod("clean");
                clean.invoke(cleaner);
            }
        } catch (Throwable ignore) {}
    }
}