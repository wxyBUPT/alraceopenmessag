package io.openmessaging.demo;

import io.openmessaging.Conf;
import io.openmessaging.MessageHeader;
import io.openmessaging.storage.MessageDecoder;
import io.openmessaging.storage.Storage;
import io.openmessaging.util.NameUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by xiyuanbupt on 5/27/17.
 */
public class ConsumerCache {

    static transient int registeredCount = 0;
    Logger logger = LoggerFactory.getLogger(ConsumerCache.class);

    private final Storage storage = Storage.getConsumerStorage();

    final List<Integer>[] index = storage.getIndex();

    final boolean[] registerd = new boolean[Conf.TOTAL_COUNT];

    transient boolean ioFinish = false;
    transient boolean decodeFinish = false;
    transient int decoderThreadCount = 0;

    public boolean isDecodeFinish(){
        return decodeFinish;
    }

    List<DefaultPullConsumer>[] router = new List[Conf.TOTAL_COUNT];
    {
        for(int i = 0; i<router.length; i++){
            router[i] = new ArrayList<>(4);
        }
    }
    final private PriorityQueue<MessageBlock> ioOrder = new PriorityQueue<>();

    public void register(String queueName, Collection<String> topics, DefaultPullConsumer consumer){
        registeredCount ++;
        registerQueue(queueName, consumer);
        registerTopic(topics, consumer);

        if(registeredCount == Conf.CONSUMER_THREAD_COUNT){
            // TODO init ioOrder
            initIoOrder();
            // TODO init shardedBlock
            for(int i=0;i<shardedBlock.length; i++){
                shardedBlock[i] = new ArrayBlockingQueue<MessageBlock>(Conf.CONSUMER_DECODE_CACHE_BLOCK_COUNT);
            }
            // TODO start io loop
            new Thread(new IOThread(), "ConsumerIOThread").start();
            // TODO start decoder thread
            decoderThreadCount += shardedBlock.length;
            for(int i=0; i<shardedBlock.length; i++){
                new Thread(new DecoderThread(i), "DecoderThread_" + i).start();
            }
        }
    }

    BlockingQueue<MessageBlock>[] shardedBlock = new BlockingQueue[Conf.CONSUMER_DECODE_THREAD_COUNT];

    class IOThread implements Runnable{
        int hashbits = Conf.CONSUMER_DECODE_THREAD_COUNT - 1;
        @Override
        public void run() {
            try {
                // 只要有io块
                while (!ioOrder.isEmpty()) {
                    // 首先读出
                    MessageBlock block = ioOrder.poll();
                    byte[] bytes = storage.getPageBytes(block.block_num);
                    block.setBytes(bytes);

                    int shard = block.code & hashbits;

                    // 无论如何都要将数据放进去
                    shardedBlock[shard].put(block);
                }
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            logger.info("IOFinish ");
            ioFinish = true;
        }
    }

    class DecoderThread implements Runnable{

        BlockingQueue<MessageBlock> blocks;

        public DecoderThread(int i){
            blocks = shardedBlock[i];
        }

        @Override
        public void run() {
            try {
                while (true) {
                    // TODO一直从自己的队列里面取数据
                    MessageBlock block = blocks.poll(200, TimeUnit.MILLISECONDS);
                    if (block != null) {
                        // TODO 解码, 并route 对应的消费者中
                        String key;
                        if (block.isTopic()) {
                            key = MessageHeader.TOPIC;
                        } else {
                            key = MessageHeader.QUEUE;
                        }
                        // TODO 将消息route到订阅的消费者
                        List<DefaultBytesMessage> messages = MessageDecoder.decodePage2Messages(block.getBytes(), key, block.name);
                        for (DefaultPullConsumer consumer : router[block.code]) {
                            consumer.messageBlocks.put(messages);
                        }
                    } else {
                        if (ioFinish) {
                            decoderThreadCount--;
                            if (decoderThreadCount == 0) {
                                logger.info("All Decoder finish ");
                                decodeFinish = true;
                            }
                            break;
                        }
                    }
                }
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    private void initIoOrder(){
        int count = registerd.length;
        for(int i=0; i<count; i++){
            if(!registerd[i])continue;
            List<Integer> blocks = index[i];
            for(int num:blocks){
                ioOrder.add(new MessageBlock(i, num));
            }
        }
    }

    private void registerQueue(String queueName, DefaultPullConsumer consumer){
        int code = NameUtil.getCode(queueName);
        registerd[code] = true;
        router[code].add(consumer);
    }

    private void registerTopic(Collection<String> topics, DefaultPullConsumer consumer){
        for(String topic:topics){
            int code = NameUtil.getCode(topic);
            registerd[code] = true;
            router[code].add(consumer);
        }
    }


    byte[] getBlockFromDisk(int blocknum){
        return storage.getPageBytes(blocknum);
    }

    static class MessageBlock implements Comparable<MessageBlock>{
        String name;
        int code;
        int block_num;
        MessageType messageType;

        byte[] bytes;

        public byte[] getBytes() {
            return bytes;
        }

        public void setBytes(byte[] bytes) {
            this.bytes= bytes;
        }

        private MessageBlock(){}

        public MessageBlock(String name, int code, int block_num, MessageType messageType) {
            this.name = name;
            this.code = code;
            this.block_num = block_num;
            this.messageType = messageType;
        }

        public MessageBlock(int code, int block_num){
            name = NameUtil.getName(code);
            this.code = code;
            this.block_num = block_num;
            if(NameUtil.isTopic(code)){
                messageType = MessageType.TOPIC;
            }else {
                messageType = MessageType.QUEUE;
            }
        }

        boolean isTopic(){
            return messageType == MessageType.TOPIC;
        }

        @Override
        public String toString() {
            return "block_num: " + block_num;
        }

        @Override
        public int compareTo(MessageBlock o) {
            return block_num - o.block_num;
        }
    }

    enum MessageType {
        TOPIC((byte)0, "Topic"), QUEUE((byte)1, "Queue");

        private byte aByte;
        private String name;

        private MessageType(byte index, String name){
            this.aByte = index;
            this.name = name;
        }

        public byte getByteValue() {
            return aByte;
        }

        @Override
        public String toString() {
            return "MessageType{" +
                    "aByte=" + aByte +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    private static ConsumerCache INSTANCE = null;
    private ConsumerCache(){}
    static public synchronized ConsumerCache getInstance(){
        if(INSTANCE == null){
            INSTANCE = new ConsumerCache();
        }
        return INSTANCE;
    }
}
