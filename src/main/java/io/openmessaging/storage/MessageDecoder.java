package io.openmessaging.storage;

import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.DefaultKeyValue;
import io.openmessaging.util.TypeUtil;

import java.util.*;

/**
 * Created by xiyuanbupt on 5/27/17.
 */
public class MessageDecoder {

    public static List<DefaultBytesMessage> decodePage2Messages(byte[] bytes, String type, String name){
        int off = 0;
        List<DefaultBytesMessage> res = new LinkedList<>();
        int len = 0;
        while ((len = TypeUtil.getInt(bytes, off)) != 0){
            off += 4;
            DefaultBytesMessage message = getMessageFromBytes(bytes, off, len);
            message.putHeaders(type, name);
            res.add(message);
            off += len;
        }
        return res;
    }

    public static DefaultBytesMessage getMessageFromBytes(byte[] bytes, int start, int len){
        int off = 0;
        DefaultKeyValue headers = new DefaultKeyValue();
        int h_s_size = TypeUtil.get2ByteInt(bytes, start + off);
        off += 2;
        if(h_s_size != 0){
            Map<String, String> h_s_map = new HashMap<>(h_s_size);
            off += getStringMap(bytes, start + off, h_s_size, h_s_map);
            headers.setString_values(h_s_map);
        }

        int h_l_size = TypeUtil.get2ByteInt(bytes, start + off);
        off += 2;
        if(h_l_size!=0){
            Map<String, Long> h_l_map = new HashMap<>(h_l_size);
            off += getLongMap(bytes, start + off, h_l_size, h_l_map);
            headers.setLong_values(h_l_map);
        }

        int h_i_size = TypeUtil.get2ByteInt(bytes, start + off);
        off += 2;
        if(h_i_size!=0){
            Map<String, Integer> h_i_map = new HashMap<>(h_i_size);
            off += getIntMap(bytes, off + start, h_i_size, h_i_map);
            headers.setInt_values(h_i_map);
        }

        DefaultKeyValue properties = new DefaultKeyValue();
        int p_s_size = TypeUtil.get2ByteInt(bytes, start + off);
        off += 2;
        if(p_s_size != 0){
            Map<String, String> p_s_map = new HashMap<>(p_s_size);
            off += getStringMap(bytes, start + off, p_s_size, p_s_map);
            properties.setString_values(p_s_map);
        }

        int p_l_size = TypeUtil.get2ByteInt(bytes, start + off);
        off += 2;
        if(p_l_size!=0){
            Map<String, Long> p_l_map = new HashMap<>(p_l_size);
            off += getLongMap(bytes, start + off, p_l_size, p_l_map);
            properties.setLong_values(p_l_map);
        }

        int p_i_size = TypeUtil.get2ByteInt(bytes, start + off);
        off += 2;
        if(p_i_size!=0){
            Map<String, Integer> p_i_map = new HashMap<>(p_i_size);
            off += getIntMap(bytes, off + start, h_i_size, p_i_map);
            properties.setInt_values(p_i_map);
        }

        int body_len = len - off;
        byte[] body = new byte[body_len];
        System.arraycopy(bytes, start + off, body, 0, body.length);

        return new DefaultBytesMessage(headers, properties, body);

    }

    private static int getStringMap(byte[] bytes, int start, int size, Map<String, String> map){
        int off = 0;
        for(int i = 0; i<size; i++){
            int k_len = TypeUtil.get2ByteInt(bytes, start + off);
            off += 2;
            String key = new String(bytes, off + start, k_len);
            off += k_len;

            int v_len = TypeUtil.get2ByteInt(bytes, start + off);
            off += 2;
            String value = new String(bytes, off + start, v_len);
            off += v_len;
            map.put(key, value);
        }
        return off;
    }

    private static int getLongMap(byte[] bytes, int start, int size ,Map<String, Long> map){
        int off = 0;
        for(int i = 0; i<size; i++){
            int k_len = TypeUtil.get2ByteInt(bytes, start + off);
            off += 2;
            String key = new String(bytes, off + start, k_len);
            off += k_len;

            long value = TypeUtil.getLong(bytes, start+off);
            off += 8;
            map.put(key, value);
        }
        return off;
    }

    private static int getIntMap(byte[] bytes, int start, int size, Map<String, Integer> map){
        int off = 0;
        for(int i = 0; i<size; i++){
            int k_len = TypeUtil.get2ByteInt(bytes, start + off);
            off += 2;
            String key = new String(bytes, off + start, k_len);
            off += k_len;

            int value = TypeUtil.getInt(bytes, start + off);
            off += 4;
            map.put(key, value);
        }
        return off;
    }
}
