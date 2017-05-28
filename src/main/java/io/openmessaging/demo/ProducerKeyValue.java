package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by xiyuanbupt on 5/28/17.
 * producer 阶段的key value
 */
public class ProducerKeyValue implements KeyValue{


    private Map<String, Integer> int_values = new HashMap<String, Integer>();

    private Map<String, Long> long_values = new HashMap<String, Long>();
    private Map<String, String> string_values = new HashMap<String, String>();

    public ProducerKeyValue() {
    }

    void clear(){
        int_values.clear();
        long_values.clear();
        string_values.clear();
    }

    public Map<String,Integer> getIntMap(){
        return int_values;
    }

    public Map<String, Long> getLongMap(){
        return long_values;
    }

    public Map<String, String>  getStringMap(){
        return string_values;
    }

    @Override
    public KeyValue put(String key, int value) {
        int_values.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        long_values.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        long_values.put(key, Double.doubleToLongBits(value));
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        string_values.put(key, value);
        return this;
    }

    @Override
    public int getInt(String key) {
        return int_values.getOrDefault(key, 0);
    }

    @Override
    public long getLong(String key) {
        return long_values.getOrDefault(key, 0L);
    }

    @Override
    public double getDouble(String key) {
        long l = long_values.getOrDefault(key, 0L);
        return Double.longBitsToDouble(l);
    }

    @Override
    public String getString(String key) {
        return string_values.getOrDefault(key, null);
    }

    @Override
    public Set<String> keySet() {
        int size = 0;
        size += long_values.size();
        size += int_values.size();
        size += string_values.size();
        Set<String> keys = new HashSet<>(size);
        keys.addAll(int_values.keySet());
        keys.addAll(long_values.keySet());
        keys.addAll(string_values.keySet());
        return keys;
    }

    @Override
    public boolean containsKey(String key) {
        return string_values.containsKey(key) ||
                long_values.containsKey(key) ||
                int_values.containsKey(key) ;
    }
}
