package io.openmessaging.demo;

import io.openmessaging.*;
import io.openmessaging.storage.ProducerPage;
import io.openmessaging.storage.ProducerStorage;
import io.openmessaging.util.NameUtil;
import io.openmessaging.util.StatusUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultProducer implements Producer{

    Logger logger = LoggerFactory.getLogger(DefaultProducer.class);

    public static AtomicInteger threadCounter = new AtomicInteger(0);
    public static CountDownLatch ioFinish = new CountDownLatch(1);
    static transient int id = -1;

    final private int m_id;

    public DefaultProducer(KeyValue properties){
        m_id = ++id;
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

    private Queue<ProducerBytesMessage> bytesMessagesPool ;
    {
        // 创建一定数量的对向
        bytesMessagesPool = new ArrayDeque<>(8);
        for(int i=0; i<1; i++){
            bytesMessagesPool.add(new ProducerBytesMessage());
        }
    }

    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        ProducerBytesMessage producerBytesMessage = bytesMessagesPool.poll();
        if(producerBytesMessage==null){
            producerBytesMessage = new ProducerBytesMessage();
        }else {
            producerBytesMessage.clear();
        }
        producerBytesMessage.setBody(body);
        producerBytesMessage.setName(topic);
        return producerBytesMessage;
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        ProducerBytesMessage producerBytesMessage = bytesMessagesPool.poll();
        if(producerBytesMessage==null){
            producerBytesMessage = new ProducerBytesMessage();
        }else {
            producerBytesMessage.clear();
        }
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
        ProducerPage page = caches[code];
        boolean saved = page.storMessage(bytesMessage);
        if(saved){
            //TODO 如果日志中size 为1, 那么不用归还, 一个线程用一个就行
            bytesMessagesPool.add(bytesMessage);
            return;
        }


        // TODO 两句都需要优化, 锁竞争应该比较激烈
        storage.putPage(m_id, page);
        ProducerPage pooledPage = storage.pollPageFromPool();

        if(pooledPage==null) {
            caches[code] = new ProducerPage(code);
        }else {
            pooledPage.setCode(code);
            pooledPage.clear();
            caches[code] = pooledPage;
        }
        caches[code].storMessage(bytesMessage);
        // TODO 如果日志中size 为1, 那么不用归还, 一个线程就用一个
        bytesMessagesPool.add(bytesMessage);
    }

    @Override
    public void flush() {
        for(ProducerPage page:caches){
            storage.putPage(m_id, page);
        }
        threadCounter.decrementAndGet();
        logger.info("Thread {} , BytesMessagePool size is {}, if size is 1, mean can use no queue", m_id, bytesMessagesPool.size());
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
