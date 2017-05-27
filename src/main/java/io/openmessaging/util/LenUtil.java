package io.openmessaging.util;

/**
 * Created by xiyuanbupt on 5/26/17.
 */
public class LenUtil {

    public static int get2BytesIntValue(byte[] bytes, int off){
        return (bytes[off++] & 0xff) << 8 | (bytes[off] & 0xff);
    }

    public static int get2BytesIntValue(byte[] bytes){
        return (bytes[0] & 0xff) << 8 | (bytes[1] & 0xff);
    }

    public static byte[] shortInt22Byte(int i){
        return new byte[]{
                (byte)((i>>8) & 0xff),
                (byte)(i & 0xff)
        };
    }

    public static void putInt22Byte(byte[] bytes, int i, int off){
        bytes[off++] = (byte)((i>>8) & 0xff);
        bytes[off] = (byte)(i & 0xff);
    }
}
