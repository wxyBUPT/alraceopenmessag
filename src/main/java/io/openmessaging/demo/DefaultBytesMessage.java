package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.util.Arrays;

/**
 * Created by xiyuanbupt on 5/26/17.
 */
public class DefaultBytesMessage implements BytesMessage{

    private DefaultKeyValue headers = new DefaultKeyValue();
    private DefaultKeyValue properties = new DefaultKeyValue();
    private byte[] body;
    String name = null;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DefaultBytesMessage(byte[] body){
        this.body = body;
        headers = new DefaultKeyValue();
        properties = new DefaultKeyValue();
    }

    public DefaultBytesMessage(){
        headers = new DefaultKeyValue();
        properties = new DefaultKeyValue();
    }


    @Override
    public String toString() {
        return "DefaultBytesMessage{" +
                "headers=" + headers +
                ", properties=" + properties +
                ", body=" + new String(body) +
                ", name='" + name + '\'' +
                '}';
    }

    public DefaultBytesMessage(DefaultKeyValue headers, DefaultKeyValue properties, byte[] body) {
        this.headers = headers;
        this.properties = properties;
        this.body = body;
    }

    @Override
    public byte[] getBody() {
        return body;
    }

    @Override
    public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override
    public KeyValue headers() {
        return headers;
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, int value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, long value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, double value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, String value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DefaultBytesMessage message = (DefaultBytesMessage) o;

        if (!headers.equals(message.headers)) return false;
        if (!properties.equals(message.properties)) return false;
        if (!Arrays.equals(body, message.body)) return false;
        return name != null ? name.equals(message.name) : message.name == null;

    }

    @Override
    public int hashCode() {
        int result = headers.hashCode();
        result = 31 * result + properties.hashCode();
        result = 31 * result + Arrays.hashCode(body);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}
