package io.openmessaging.demo;

import io.openmessaging.Conf;
import io.openmessaging.storage.ProducerPage;
import io.openmessaging.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by xiyuanbupt on 5/26/17.
 * 用于缓存消息
 */
public class ProducerCache {

    Logger logger = LoggerFactory.getLogger(ProducerCache.class);

    private final int total_num = Conf.TOTAL_COUNT;

    private ProducerPage[] caches = new ProducerPage[total_num];
    {
        for(int i = 0; i<total_num; i++){
            caches[i] = new ProducerPage();
        }
    }

    // 用于保存对应的消息都保存在哪些page块中
    private final List<Integer>[] index = new List[total_num];
    {
        for(int i = 0; i<total_num; i++){
            index[i] = new ArrayList<>(32);
        }
    }

    private final Storage storage = Storage.getProducerStorage();

    private static ProducerCache INSTANCE = null;
    private ProducerCache(){}
    static public synchronized ProducerCache getInstance(){
        if(INSTANCE == null){
            INSTANCE = new ProducerCache();
        }
        return INSTANCE;
    }

    public void putMessage(int code, DefaultBytesMessage message){
        synchronized (caches[code]){
            ProducerPage page = caches[code];
            boolean saved = page.storMessage(message);
            // 保存到了cache
            if(saved)return;
            // 保存到哪里
            int block_num = storage.storePageBytes(page.getBytes());
            index[code].add(block_num);
            // 清空cache
            page.clear();
            saved = page.storMessage(message);
            if(!saved){
                logger.info("Can't save message, some wrone may happen, message {}", message.toString());
            }
        }
    }

    public void storeAll(){
        for(int i =0; i<caches.length; i++){
            ProducerPage page = caches[i];
            if(!page.isEmpty()){
                int block_num = storage.storePageBytes(page.getBytes());
                index[i].add(block_num);
            }
        }
        storage.storeIndex(index);
    }

}
