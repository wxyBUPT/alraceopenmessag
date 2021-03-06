package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.util.Arrays;

/**
 * Created by xiyuanbupt on 5/26/17.
 */
public class DefaultBytesMessage implements BytesMessage{

    private DefaultKeyValue headers;
    private DefaultKeyValue properties;
    private byte[] body;
    String name = null;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private DefaultBytesMessage(){}

    public DefaultBytesMessage(byte[] body){
        this.body = body;
        headers = new DefaultKeyValue();
        properties = new DefaultKeyValue();
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
}
