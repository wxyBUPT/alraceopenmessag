package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.demo.ConsumerCache;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.util.StatusUtil;

import java.util.List;

/**
 * Created by xiyuanbupt on 5/27/17.
 * 测试在磁盘中顺序读
 */
public class MyTester {

    public static void main(String[] args){
        try {
            StatusUtil.init("/Users/xiyuanbupt/ssd_mq_store", false);
        }catch (Exception e){
            e.printStackTrace();
        }
        ConsumerCache consumerCache = ConsumerCache.getInstance();
        List<DefaultBytesMessage> messages = consumerCache.getTopicBlockFromLRUOrDisk(0, "key", "value");
        for(Message message:messages){
            System.out.println(message);
        }
    }
}
