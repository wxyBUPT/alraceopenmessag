package io.openmessaging.demo;

import io.openmessaging.*;
import io.openmessaging.storage.ProducerPage;
import io.openmessaging.storage.ProducerStorage;
import io.openmessaging.util.NameUtil;
import io.openmessaging.util.StatusUtil;

import java.util.concurrent.atomic.AtomicInteger;

public class DefaultProducer implements Producer{


    public static AtomicInteger threadCounter = new AtomicInteger(0);
    static int id = -1;

    final private int m_id;

    public DefaultProducer(KeyValue properties){
        m_id = ++id;
        threadCounter.incrementAndGet();
        StatusUtil.init(properties.getString("STORE_PATH"), true);

        storage = ProducerStorage.getInstance();
    }

    static private final int total_num = Conf.TOTAL_COUNT;

    private ProducerPage[] caches = new ProducerPage[total_num];
    {
        for(int i = 0; i<total_num; i++){
            caches[i] = new ProducerPage(i);
        }
    }

    private final ProducerStorage storage;


    ProducerBytesMessage producerBytesMessage = new ProducerBytesMessage();

    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        producerBytesMessage.clear();
        producerBytesMessage.setBody(body);
        producerBytesMessage.setName(topic);
        return producerBytesMessage;
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        producerBytesMessage.clear();
        producerBytesMessage.setBody(body);
        producerBytesMessage.setName(queue);
        return producerBytesMessage;
    }

    @Override
    public KeyValue properties() {
        return null;
    }

    @Override
    public void send(Message message) {
        ProducerBytesMessage bytesMessage = (ProducerBytesMessage) message;
        String name = bytesMessage.getName();
        int code = NameUtil.getCode(name);
        if(caches[code].storMessage(bytesMessage)){
            return;
        }

        storage.storePage(m_id, caches[code]);
        caches[code].clear();
        caches[code].storMessage(bytesMessage);
    }

    @Override
    public void flush() {
        for(ProducerPage page:caches){
            storage.storePage(m_id, page);
        }
        if(threadCounter.decrementAndGet()==0){
            storage.finish();
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
