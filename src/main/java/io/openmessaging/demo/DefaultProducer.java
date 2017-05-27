package io.openmessaging.demo;

import io.openmessaging.*;
import io.openmessaging.util.NameUtil;
import io.openmessaging.util.StatusUtil;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultProducer implements Producer{
    static AtomicInteger threadCounter = new AtomicInteger(Conf.PRODUCER_COUNT);

    ProducerCache producerCache;

    public DefaultProducer(KeyValue properties){
        try {
            StatusUtil.init(properties.getString("STORE_PATH"), true);
        }catch (IOException e){
            e.printStackTrace();
            System.exit(-1);
        }
        producerCache = ProducerCache.getInstance();
    }

    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.setName(topic);
        return defaultBytesMessage;
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.setName(queue);
        return defaultBytesMessage;
    }

    @Override
    public KeyValue properties() {
        return null;
    }

    @Override
    public void send(Message message) {
        if (message == null) return;
        DefaultBytesMessage bytesMessage = (DefaultBytesMessage)message;
        String name = bytesMessage.getName();
        producerCache.putMessage(NameUtil.getCode(name), bytesMessage);
    }

    @Override
    public void flush() {
        if(threadCounter.decrementAndGet()==0){
            System.out.println("storealll !");
            producerCache.storeAll();
        }
    }


    @Override
    public void send(Message message, KeyValue properties) {

    }

    @Override
    public Promise<Void> sendAsync(Message message) {
        return null;
    }

    @Override
    public Promise<Void> sendAsync(Message message, KeyValue properties) {
        return null;
    }

    @Override
    public void sendOneway(Message message) {

    }

    @Override
    public void sendOneway(Message message, KeyValue properties) {

    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName) {
        return null;
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        return null;
    }


    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }
}
