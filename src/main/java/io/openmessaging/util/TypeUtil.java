package io.openmessaging.util;

import java.nio.ByteBuffer;

/**
 * Created by xiyuanbupt on 5/26/17.
 */
public class TypeUtil {

    // int 与 byte array 之间的转换
    // 按照int二进制顺序存储
    public static byte[] intToBytes(int i){
        return new byte[]{
                (byte)((i>>24) & 0xff),
                (byte)((i>>16) & 0xff),
                (byte)((i>>8) & 0xff),
                (byte)((i) & 0xff)
        };
    }
    public static int bytesToInt(byte[] b)
    {
        return   b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    public static void writeInt(byte[] bytes, int num, int start)
    {
        bytes[start++] = (byte)((num >> 24) & 0xff);
        bytes[start++] = (byte)((num >> 16) & 0xff);
        bytes[start++] = (byte)((num >> 8) & 0xff);
        bytes[start] = (byte)((num) & 0xff);
    }

    public static int getInt(byte[] bytes, int start){
        return (bytes[start++] & 0xff) << 24 | (bytes[start++] & 0xff) << 16 | (bytes[start++] & 0xff) << 8 | (bytes[start] & 0xff);
    }

    // 2byte数和int之间的转换
    public static void write2byteInt(byte[] bytes, int i, int start){
        bytes[start++] = (byte) ((i >> 8) & 0xff);
        bytes[start] = (byte)(i & 0xff);
    }

    public static int get2ByteInt(byte[] bytes, int start){
        return (bytes[start++] & 0xff) << 8 | (bytes[start] & 0xff);
    }

    // long 与 byte array 之间的转换
    public static byte[] longToBytes(long l)
    {
        return new byte[]{
                (byte)((l >> 56) & 0xff),
                (byte)((l >> 48) & 0xff),
                (byte)((l >> 40) & 0xff),
                (byte)((l >> 32) & 0xff),
                (byte)((l >> 24) & 0xff),
                (byte)((l >> 16) & 0xff),
                (byte)((l >> 8) & 0xff),
                (byte)((l) & 0xff)
        };
    }

    public static long bytesToLong(byte[] b)
    {
        return   (long)b[7] & 0xff |
                ((long)b[6] & 0xff) << 8 |
                ((long)b[5] & 0xff) << 16|
                ((long)b[4] & 0xff) << 24|
                ((long)b[3] & 0xff) << 32|
                ((long)b[2] & 0xff) << 40|
                ((long)b[1] & 0xff) << 48|
                ((long)b[0] & 0xff) << 56;
    }

    public static void writeLong(byte[] bytes, long l, int start)
    {
        bytes[start++] = (byte)((l >> 56) & 0xff);
        bytes[start++] = (byte)((l >> 48) & 0xff);
        bytes[start++] = (byte)((l >> 40) & 0xff);
        bytes[start++] = (byte)((l >> 32) & 0xff);
        bytes[start++] = (byte)((l >> 24) & 0xff);
        bytes[start++] = (byte)((l >> 16) & 0xff);
        bytes[start++] = (byte)((l >> 8) & 0xff);
        bytes[start] = (byte)((l) & 0xff);
    }

    public static long getLong(byte[] bytes, int start)
    {
        return ((long)bytes[start++] & 0xff) << 56 |
                ((long)bytes[start++] & 0xff) << 48 |
                ((long)bytes[start++] & 0xff) << 40 |
                ((long)bytes[start++] & 0xff) << 32 |
                ((long)bytes[start++] & 0xff) << 24|
                ((long)bytes[start++] & 0xff) << 16 |
                ((long)bytes[start++] & 0xff) << 8 |
                ((long)bytes[start] & 0xff);
    }

    // double与byte array 之间的转换
    public static byte[] double2Bytes(double d){
        long l = Double.doubleToLongBits(d);
        return longToBytes(l);
    }

    public static double bytes2Double(byte[] bytes){
        long l = bytesToLong(bytes);
        return Double.longBitsToDouble(l);
    }

    public static void writeDouble(byte[] bytes, double d, int start){
        long l = Double.doubleToLongBits(d);
        writeLong(bytes, l, start);
    }

    public static double getDouble(byte[] bytes, int start){
        long l = getLong(bytes, start);
        return Double.longBitsToDouble(l);
    }

    public static byte[] shortToBytes(short number) {
        int temp = number;
        byte[] b = new byte[2];
        for (int i = b.length - 1; i >=0; i--) {
            b[i] = (byte) (temp & 0xff);
            temp = temp >>> 8; // 向右移8位
        }
        return b;
    }

}
