package io.openmessaging.demo;

/**
 * Created by xiyuanbupt on 5/27/17.
 */
import io.openmessaging.*;
import io.openmessaging.storage.ConsumerStorage;
import io.openmessaging.storage.MessageDecoder;
import io.openmessaging.util.NameUtil;
import io.openmessaging.util.StatusUtil;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DefaultPullConsumer implements PullConsumer{

    ConsumerCache consumerCache;
    private final ConsumerStorage consumerStorage ;
    byte[] bytes = new byte[Conf.PAGE_SIZE];

    public DefaultPullConsumer(KeyValue properties){
        StatusUtil.init(properties.getString("STORE_PATH"), false);
        consumerCache = ConsumerCache.getInstance();
        consumerStorage = ConsumerStorage.getConsumerStorage();
    }

    final private PriorityQueue<ConsumerCache.MessageBlock> ioOrder = new PriorityQueue<>(128);

    BlockingQueue<List<DefaultBytesMessage>> messageBlocks = new LinkedBlockingQueue<>(Conf.CONSUMER_CACHE_BLOCK_SIZE);

    // 需要自己处理io的数据code
    List<Integer> ioCodes = new ArrayList<>(16);

    void realAttachTopic(int i){
        ioCodes.add(i);
    }

    void initIOOrder(){
        for(int i:ioCodes){
            List<Integer> ioBlocks = consumerCache.index[i];
            for (int num : ioBlocks) {
                ioOrder.add(new ConsumerCache.MessageBlock(i, num));
            }
        }
    }

    @Override
    public void attachQueue(String queueName, Collection<String> topics) {
        ioCodes.add(NameUtil.getCode(queueName));
        consumerCache.register(topics, this);
    }


    Iterator<DefaultBytesMessage> iterator = new ArrayList<DefaultBytesMessage>().iterator();

    @Override
    public KeyValue properties() {
        return null;
    }

    @Override
    public Message poll() {
        if(iterator.hasNext()){
            return iterator.next();
        }
        if(updateIterator()){
            return iterator.next();
        }
        else {
            return null;
        }
    }

    private boolean updateIterator(){
        List<DefaultBytesMessage> messages = null;
        // TODO 首先读自己的部分
        if(!ioOrder.isEmpty()){
            ConsumerCache.MessageBlock block = ioOrder.poll();
            consumerStorage.getPageBytes(block.block_num, bytes);
            String key = null;
            if(block.isTopic()){
                key = MessageHeader.TOPIC;
            }else {
                key = MessageHeader.QUEUE;
            }
            iterator = MessageDecoder.decodePage2Messages(bytes, key, block.name).iterator();
            return true;
        }
        try {
            while (true) {
                messages = messageBlocks.poll(200, TimeUnit.MILLISECONDS);
                if (messages != null) {
                    iterator = messages.iterator();
                    return true;
                }
                if (consumerCache.isDecodeFinish()) {
                    return false;
                }
            }
        }catch (InterruptedException e){
            e.printStackTrace();
            // 如果被中断, 那么执行结束
            return false;
        }
    }

    @Override
    public Message poll(KeyValue properties) {
        return null;
    }

    @Override
    public void ack(String messageId) {

    }

    @Override
    public void ack(String messageId, KeyValue properties) {

    }
}