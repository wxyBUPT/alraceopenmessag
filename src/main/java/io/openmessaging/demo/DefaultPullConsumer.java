package io.openmessaging.demo;

/**
 * Created by xiyuanbupt on 5/27/17.
 */
import io.openmessaging.*;
import io.openmessaging.util.StatusUtil;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DefaultPullConsumer implements PullConsumer{

    ConsumerCache consumerCache;

    public DefaultPullConsumer(KeyValue properties){
        try{
            StatusUtil.init(properties.getString("STORE_PATH"), false);
        }catch (IOException e){
            e.printStackTrace();
            System.exit(-1);
        }
        consumerCache = ConsumerCache.getInstance();
    }

    BlockingQueue<List<DefaultBytesMessage>> messageBlocks = new ArrayBlockingQueue<List<DefaultBytesMessage>>(Conf.CONSUMER_CACHE_BLOCK_SIZE);

    @Override
    public void attachQueue(String queueName, Collection<String> topics) {
        consumerCache.register(queueName, topics, this);
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