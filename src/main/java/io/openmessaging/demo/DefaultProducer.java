package io.openmessaging.demo;

import io.openmessaging.*;
import io.openmessaging.storage.ProducerPage;
import io.openmessaging.storage.ProducerStorage;
import io.openmessaging.util.NameUtil;
import io.openmessaging.util.StatusUtil;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultProducer implements Producer{
    public static AtomicInteger threadCounter = new AtomicInteger(0);
    public static CountDownLatch ioFinish = new CountDownLatch(1);

    public DefaultProducer(KeyValue properties){
        threadCounter.incrementAndGet();
        try {
            StatusUtil.init(properties.getString("STORE_PATH"), true);
        }catch (IOException e){
            e.printStackTrace();
            System.exit(-1);
        }
        storage = ProducerStorage.getInstance();
    }

    private final int total_num = Conf.TOTAL_COUNT;

    private ProducerPage[] caches = new ProducerPage[total_num];
    {
        for(int i = 0; i<total_num; i++){
            caches[i] = new ProducerPage(i);
        }
    }

    private final ProducerStorage storage;

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
        int code = NameUtil.getCode(name);
        ProducerPage page = caches[code];
        boolean saved = page.storMessage(bytesMessage);
        if(saved)return;

        // 如果放不下当前消息, 如果缓存队列满了, 则会一直阻塞
        storage.putPage(page);

        caches[code] = new ProducerPage(code);
        caches[code].storMessage(bytesMessage);
    }

    @Override
    public void flush() {
        //TODO 保存所有的消息
        for(ProducerPage page:caches){
            storage.putPage(page);
        }
        threadCounter.decrementAndGet();
        try {
            ioFinish.await();
        }catch (InterruptedException e){
            e.printStackTrace();
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
