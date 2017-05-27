package io.openmessaging.demo;

import io.openmessaging.storage.MessageDecoder;
import io.openmessaging.storage.Storage;

import java.util.*;

/**
 * Created by xiyuanbupt on 5/27/17.
 */
public class ConsumerCache {

    private final Storage storage = Storage.getConsumerStorage();

    final List<Integer>[] index = storage.getIndex();

    byte[] getBlockFromDisk(int blocknum){
        return storage.getPageBytes(blocknum);
    }
    List<DefaultBytesMessage> getTopicBlockFromLRUOrDisk(int blocknum, String h_key, String h_value){
        // TODO 添加lru cache
        byte[] bytes = getBlockFromDisk(blocknum);
        return MessageDecoder.decodePage2Messages(bytes, h_key, h_value);
    }

    static class MessageBlock implements Comparable<MessageBlock>{
        String name;
        int code;
        int block_num;
        MessageType messageType;

        public MessageBlock(String name, int code, int block_num, MessageType messageType) {
            this.name = name;
            this.code = code;
            this.block_num = block_num;
            this.messageType = messageType;
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
