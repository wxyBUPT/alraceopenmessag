package io.openmessaging.storage;

import io.openmessaging.Conf;
import io.openmessaging.demo.ProducerBytesMessage;

/**
 * Created by xiyuanbupt on 5/26/17.
 * 约定
 * 1. 不能再多线程的情况下使用page
 * 2. 只能在produce中使用
 */
public class ProducerPage {
    int pageSize = Conf.PAGE_SIZE;// 128K
    byte[] bytes = new byte[pageSize];
    // 第一个没有被用到的消息位
    int pos = 0;

    // 必须使用code初始化
    private ProducerPage(){}

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public ProducerPage(int code) {
        this.code = code;
    }

    int code;

    public void clear(){
        pos = 0;
    }
    public boolean isEmpty(){
        return pos == 0;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public boolean storMessage(ProducerBytesMessage message){
        // TODO 将消息保存到page中, 如果保存不下, 返回false
        return MessageEncoder.encodeMessageToPage(this, message);
    }
}
