package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

/**
 * Created by xiyuanbupt on 5/28/17.
 * producer 阶段的byte message
 * 1. 因为在producer阶段生命周期较短, 所以应该为对象复用考虑
 */
public class ProducerBytesMessage implements BytesMessage{

    private ProducerKeyValue headers = new ProducerKeyValue();
    private ProducerKeyValue properties = new ProducerKeyValue();
    private byte[] body = null;
    String name = null;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ProducerBytesMessage(){}

    public void clear(){
        headers.clear();
        properties.clear();
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
