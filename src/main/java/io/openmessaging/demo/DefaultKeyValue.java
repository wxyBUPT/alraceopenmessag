package io.openmessaging.demo;

/**
 * Created by xiyuanbupt on 5/26/17.
 */

import io.openmessaging.KeyValue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by xiyuanbupt on 4/11/17.
 * KeyValue 的默认实现, 对于每一类默认使用对应的字典
 * 约定:
 * 1. 只有在Consumer阶段可以使用
 */
public final class DefaultKeyValue implements KeyValue {

    private Map<String, Integer> int_values;

    private Map<String, Long> long_values;
    private Map<String, String> string_values;

    // 做了多方权衡, 觉得应该初始化为null
    public DefaultKeyValue() {
        int_values = null;
        long_values = null;
        string_values = null;
    }

    public void setInt_values(Map<String, Integer> int_values) {
        this.int_values = int_values;
    }

    public void setLong_values(Map<String, Long> long_values) {
        this.long_values = long_values;
    }

    public void setString_values(Map<String, String> string_values) {
        this.string_values = string_values;
    }

    @Override
    public KeyValue put(String key, int value) {
        if(int_values==null){
            int_values = new HashMap<>();
        }
        int_values.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        if(long_values == null){
            long_values = new HashMap<>();
        }
        long_values.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        if(long_values == null){
            long_values = new HashMap<>();
        }
        long_values.put(key, Double.doubleToLongBits(value));
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        if(string_values==null){
            string_values = new HashMap<>();
        }
        string_values.put(key, value);
        return this;
    }

    @Override
    public int getInt(String key) {
        if(int_values==null)return 0;
        return int_values.getOrDefault(key, 0);
    }

    @Override
    public long getLong(String key) {
        if(long_values==null)return 0L;
        return long_values.getOrDefault(key, 0L);
    }

    @Override
    public double getDouble(String key) {
        if(long_values==null)return 0.0d;
        long l = long_values.getOrDefault(key, 0L);
        return Double.longBitsToDouble(l);
    }

    @Override
    public String getString(String key) {
        if(string_values==null)return null;
        return string_values.getOrDefault(key, null);
    }

    @Override
    public Set<String> keySet() {
        int size = 0;
        if(int_values!=null){
            size += int_values.size();
        }
        if(long_values != null){
            size += long_values.size();
        }
        if(string_values != null){
            size += string_values.size();
        }
        Set<String> keys = new HashSet<>(size);
        if(int_values!=null) {
            keys.addAll(int_values.keySet());
        }
        if(long_values != null) {
            keys.addAll(long_values.keySet());
        }
        if(string_values!=null) {
            keys.addAll(string_values.keySet());
        }
        return keys;
    }

    @Override
    public boolean containsKey(String key) {
        return
                (string_values != null && string_values.containsKey(key))||
                        (int_values != null && int_values.containsKey(key) )||
                        (long_values != null && long_values.containsKey(key));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DefaultKeyValue that = (DefaultKeyValue) o;

        if (int_values != null ? !int_values.equals(that.int_values) : that.int_values != null) return false;
        if (long_values != null ? !long_values.equals(that.long_values) : that.long_values != null) return false;
        return string_values != null ? string_values.equals(that.string_values) : that.string_values == null;

    }

    @Override
    public int hashCode() {
        int result = int_values != null ? int_values.hashCode() : 0;
        result = 31 * result + (long_values != null ? long_values.hashCode() : 0);
        result = 31 * result + (string_values != null ? string_values.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DefaultKeyValue{" +
                "int_values=" + int_values +
                ", long_values=" + long_values +
                ", string_values=" + string_values +
                '}';
    }
}
