package io.openmessaging.demo;

/**
 * Created by xiyuanbupt on 5/27/17.
 */
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;
import io.openmessaging.storage.MessageDecoder;
import io.openmessaging.util.NameUtil;
import io.openmessaging.util.StatusUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class DefaultPullConsumer implements PullConsumer{

    Logger logger = LoggerFactory.getLogger(DefaultPullConsumer.class);


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

    List<Message> firstMess = null;

    @Override
    public void attachQueue(String queueName, Collection<String> topics) {
        ioOrder = new PriorityQueue<>();
        // 放入index
        int queueCode = NameUtil.getCode(queueName);
        List<Integer> queueBlocks = consumerCache.index[queueCode];
        for(int num:queueBlocks){
            ioOrder.add(new ConsumerCache.MessageBlock(queueName, queueCode, num, ConsumerCache.MessageType.QUEUE));
        }
        for(String topic:topics){
            int topicCode = NameUtil.getCode(topic);
            List<Integer> topicBlocks = consumerCache.index[topicCode];
            for(int num:topicBlocks){
                ioOrder.add(new ConsumerCache.MessageBlock(topic, topicCode, num, ConsumerCache.MessageType.TOPIC));
            }
        }
    }
    Iterator<DefaultBytesMessage> iterator = new ArrayList<DefaultBytesMessage>().iterator();

    private PriorityQueue<ConsumerCache.MessageBlock> ioOrder = null;

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
        if(ioOrder.isEmpty())return false;
        ConsumerCache.MessageBlock block = ioOrder.poll();
        if(block.isTopic()){
            iterator = consumerCache.getTopicBlockFromLRUOrDisk(block.block_num, MessageHeader.TOPIC, block.name).iterator();
        }else {
            byte[] bytes = consumerCache.getBlockFromDisk(block.block_num);
            List<DefaultBytesMessage> messages = MessageDecoder.decodePage2Messages(bytes, MessageHeader.QUEUE, block.name);
            iterator = messages.iterator();
        }
        return true;
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