package io.openmessaging.ringbuffer;

import io.openmessaging.Message;

/**
 * Created by xiyuanbupt on 5/26/17.
 * 约定
 * 1. 不能在多线程的情况下使用.
 */
public class RingBuffer {

    private final static int bufferSize = 0x400;
    private Message[] buffer = new Message[bufferSize];
    private int head = 0;
    private int tail = 0;

    private Boolean empty() {
        return head == tail;
    }
    private Boolean full() {
        return (tail + 1) % bufferSize == head;
    }
    public Boolean put(Message v) {
        if (full()) {
            return false;
        }
        buffer[tail] = v;
        tail = (tail + 1) & 0x3ff;
        return true;
    }
    public Message get() {
        if (empty()) {
            return null;
        }
        Message result = buffer[head];
        head = (head + 1) & 0x3ff;
        return result;
    }

    public Message peek(){
        if(empty()){
            return null;
        }
        return buffer[head];
    }

    public static void main(String[] args){
        System.out.println(bufferSize);
    }

    // 下面方法不到万不可以不要使用
    public Message[] getAll() {
        if (empty()) {
            return new Message[0];
        }
        int copyTail = tail;
        int cnt = head < copyTail ? copyTail - head : bufferSize - head + copyTail;
        Message[] result = new Message[cnt];
        if (head < copyTail) {
            for (int i = head; i < copyTail; i++) {
                result[i - head] = buffer[i];
            }
        } else {
            for (int i = head; i < bufferSize; i++) {
                result[i - head] = buffer[i];
            }
            for (int i = 0; i < copyTail; i++) {
                result[bufferSize - head + i] = buffer[i];
            }
        }
        head = copyTail;
        return result;
    }
}
