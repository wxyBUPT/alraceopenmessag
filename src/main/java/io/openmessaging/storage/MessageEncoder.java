package io.openmessaging.storage;

import io.openmessaging.Conf;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.DefaultKeyValue;
import io.openmessaging.demo.ProducerBytesMessage;
import io.openmessaging.demo.ProducerKeyValue;
import io.openmessaging.util.TypeUtil;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by xiyuanbupt on 5/26/17.
 * 约定
 * 1. 需要实现消息的序列化, 同时将序列化之后的消息保存到page中
 */
public class MessageEncoder {

    final private static int page_safe_edage = Conf.PAGE_SAFE_EDAGE;
    /**
     * 4byte 消息长度
     * header 部分
     * 2 byte string kv个数
     * 对于每个 string kv
     * 2 byte key 长度 | key的byte值 | 2byte长度 | value的byte值
     * 2 byte long kv 个数
     * 对于每个long kv
     * 2 byte key 长度 | key的byte值 | 8byte long值 |
     * 对于double和int道理是一样的
     */
    public static boolean encodeMessageToPage(ProducerPage producerPage, ProducerBytesMessage message){
        int start = producerPage.pos;
        byte[] bytes = producerPage.bytes;

        // 如果有保存不下的风险
        if((start + page_safe_edage) > bytes.length)return false;

        ProducerKeyValue headers = (ProducerKeyValue) message.headers();
        ProducerKeyValue properties = (ProducerKeyValue) message.properties();
        // 保留4byte的长度
        int off = 4;
        int h_off = writeDefaultKeyValue(bytes, start + off, headers);
        if(h_off==-1)return false;
        off += h_off;

        int p_off = writeDefaultKeyValue(bytes, start + off, properties);
        if(p_off == -1)return false;
        off += p_off;

        byte[] body = message.getBody();
        // 如果不能写下body, 与下一条消息长度(0 或者其他int值)
        if((start + off + body.length + 4) > bytes.length )return false;
        System.arraycopy(body, 0, bytes, start + off, body.length);
        off += body.length;
        // 需要在后面记录下一条长度, 如果为0 则代表下一条为空
        TypeUtil.writeInt(bytes, 0, start + off);
        producerPage.pos += off;
        TypeUtil.writeInt(bytes, off - 4, start);
        return true;
    }

    private static int writeDefaultKeyValue(byte[] bytes, int start, ProducerKeyValue keyValue){
        Map<String, String> strMap = keyValue.getStringMap();
        Map<String, Long> lonMap = keyValue.getLongMap();
        Map<String, Integer> intMap = keyValue.getIntMap();
        int off = 0;
        int s_off = writeStringMap(bytes, start + off, strMap);
        if(s_off==-1)return -1;
        off += s_off;
        int l_off = writeLongMap(bytes, start + off, lonMap);
        if(l_off==-1)return -1;
        off += l_off;
        int i_off = writeIntMap(bytes, start + off, intMap);
        if(i_off == -1)return -1;
        off += i_off;
        return off;
    }

    private static int writeStringMap(byte[] bytes, int start, Map<String, String> map){
        int len = bytes.length;
        if(start + 2 > len)return -1;
        /**
        if(map == null){
            TypeUtil.write2byteInt(bytes, 0, start);
            return 2;
        }
         **/
        int off = 0;
        int size = map.size();
        TypeUtil.write2byteInt(bytes, size, start);
        off += 2;
        for(Map.Entry<String, String> entry:map.entrySet()){
            byte[] key = entry.getKey().getBytes();
            byte[] value = entry.getValue().getBytes();
            if(start + off + 4 + key.length + value.length > len)return -1;
            // key长度
            TypeUtil.write2byteInt(bytes, key.length, start + off);
            off += 2;
            // key
            System.arraycopy(key, 0, bytes, start + off, key.length);
            off += key.length;
            // value 长度
            TypeUtil.write2byteInt(bytes, value.length, start + off);
            off += 2;
            // value
            System.arraycopy(value, 0, bytes, start + off, value.length);
            off += value.length;
        }
        return off;
    }

    private static int writeLongMap(byte[] bytes, int start, Map<String, Long> map){
        int len = bytes.length;
        if(start + 2 > len)return -1;
        /**
        if(map == null){
            TypeUtil.write2byteInt(bytes, 0, start);
            return 2;
        }
         **/
        int off = 0;
        int size = map.size();
        TypeUtil.write2byteInt(bytes, size, start);
        off += 2;

        for(Map.Entry<String, Long> entry:map.entrySet()){
            byte[] key = entry.getKey().getBytes();
            if(start + off + key.length + 10 > len)return -1;
            // key 长度
            TypeUtil.write2byteInt(bytes, key.length, start + off);
            off += 2;
            // key
            System.arraycopy(key, 0, bytes, start + off, key.length);
            off += key.length;
            // 8bit value
            TypeUtil.writeLong(bytes, entry.getValue(), start + off);
            off += 8;
        }
        return off;
    }

    private static int writeIntMap(byte[] bytes, int start, Map<String, Integer> map){
        int len = bytes.length;
        if(start + 2 > len)return -1;
        /**
        if(map == null){
            TypeUtil.write2byteInt(bytes, 0, start);
            return 2;
        }
         **/
        int off = 0;
        int size = map.size();
        TypeUtil.write2byteInt(bytes, size, start);
        off += 2;

        for(Map.Entry<String, Integer> entry:map.entrySet()){
            byte[] key = entry.getKey().getBytes();
            if(start + off + key.length + 6 > len)return -1;
            // key 长度
            TypeUtil.write2byteInt(bytes, key.length, start + off);
            off += 2;
            // key
            System.arraycopy(key, 0, bytes, start + off, key.length);
            off += key.length;
            // 4bit value
            TypeUtil.writeInt(bytes, entry.getValue(), start + off);
            off += 4;
        }
        return off;
    }
}