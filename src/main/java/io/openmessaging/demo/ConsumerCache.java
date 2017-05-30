package io.openmessaging.demo;

import io.openmessaging.Conf;
import io.openmessaging.MessageHeader;
import io.openmessaging.storage.MessageDecoder;
import io.openmessaging.storage.ConsumerStorage;
import io.openmessaging.util.NameUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by xiyuanbupt on 5/27/17.
 * 负责处理多个consumer共同订阅的topic的io 与编码解码工作
 */
public class ConsumerCache {

    static transient int registeredCount = 0;
    Logger logger = LoggerFactory.getLogger(ConsumerCache.class);

    private final ConsumerStorage consumerStorage = ConsumerStorage.getConsumerStorage();

    final List<Integer>[] index = consumerStorage.getIndex();

    Set<DefaultPullConsumer> consumers = new HashSet<>(16);

    transient boolean decodeFinish = false;
    transient int n_cache_thread_running = 0;

    // cache使用的解码线程的数量
    final int share_decode_count = Conf.SHARE_DECODE_COUNT ;
    final int count_per_thread = Conf.TOPIC_COUNT / share_decode_count ;
    {
        assert Conf.TOPIC_COUNT % share_decode_count == 0;
    }


    public boolean isDecodeFinish(){
        return decodeFinish;
    }

    List<DefaultPullConsumer>[] router = new List[Conf.TOPIC_COUNT];
    {
        for(int i = 0; i<router.length; i++){
            router[i] = new ArrayList<>(4);
        }
    }
    final private PriorityQueue<MessageBlock>[] ioOrders = new PriorityQueue[share_decode_count];
    {
        for(int i = 0; i<share_decode_count; i++){
            ioOrders[i] = new PriorityQueue<>(128);
        }
    }

    public void register(Collection<String> topics, DefaultPullConsumer consumer){
        registeredCount ++;
        registerTopic(topics, consumer);
        consumers.add(consumer);

        if(registeredCount == Conf.CONSUMER_THREAD_COUNT){
            initIoOrder();
            // TODO start topic decode and io thread
            Thread[] threads = new Thread[share_decode_count];
            for(int i=0;i<share_decode_count; i++){
                threads[i] = new Thread(new CachedTopicIOThread(i), "CachedTopicIOThread" + i);
            }
            for(Thread thread:threads){
                thread.start();
            }
        }
    }

    private void registerTopic(Collection<String> topics, DefaultPullConsumer consumer){
        for(String topic:topics){
            int code = NameUtil.getCode(topic);
            router[code].add(consumer);
        }
    }

    private void initIoOrder(){
        int len = router.length;
        assert len % share_decode_count == 0;


        for(int i = 0; i<len ;i++){
            List<DefaultPullConsumer> consumers = router[i];
            if(consumers.size() == 0)continue;// 没有订阅

            if(consumers.size()>1){// 订阅数量大于1
                List<Integer> blocks = index[i];
                for(int num:blocks){
                    ioOrders[i/count_per_thread].add(new MessageBlock(i, num));
                }
            }else {// 只有一个订阅
                router[i].get(0).realAttachTopic(i);
                router[i].clear();
            }
        }
        for(DefaultPullConsumer consumer:consumers){
            consumer.initIOOrder();
        }
    }

    int page_size = Conf.PAGE_SIZE;

    class CachedTopicIOThread implements Runnable{
        PriorityQueue<MessageBlock> ioOrder;
        CachedTopicIOThread(int i){
            ioOrder = ioOrders[i];
            n_cache_thread_running++;
        }
        byte[] bytes = new byte[page_size];
        @Override
        public void run() {
            try {
                while (!ioOrder.isEmpty()) {
                    MessageBlock block = ioOrder.poll();
                    // 从磁盘中读出数据
                    consumerStorage.getPageBytes(block.block_num, bytes);
                    // 解码
                    String key = null;
                    if (block.isTopic()) {
                        key = MessageHeader.TOPIC;
                    } else {
                        key = MessageHeader.QUEUE;
                    }
                    List<DefaultBytesMessage> messages = MessageDecoder.decodePage2Messages(bytes, key, block.name);

                    // TODO 向所有的consumer放入数据块
                    for (DefaultPullConsumer consumer : router[block.code]) {
                        consumer.messageBlocks.put(messages);
                    }
                }

            }catch (InterruptedException e){
                e.printStackTrace();
            }

            n_cache_thread_running--;
            if(n_cache_thread_running==0){
                logger.info("All cached decode and io finish ");
                decodeFinish = true;
            }
        }
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
