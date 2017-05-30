package io.openmessaging.storage;

import io.openmessaging.Conf;
import io.openmessaging.util.StatusUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiyuanbupt on 5/26/17.
 * 约定
 * 1. 需要与上层应用协商好pagesize
 * 2. 只负责高效的存储 byte[] 形式的page, 并返回page标号, 以及通过标号取出byte[] 形式的page
 */
public abstract class ConsumerStorage {

    static final int page_size = Conf.PAGE_SIZE;
    static final int file_size = Conf.FILE_SIZE;
    static final int n_block_per_file = file_size / page_size;
    static final String file_prefix = StatusUtil.getFilePath() + File.separator + "mq.";


    ConsumerStorage(){

    }

    static ConsumerStorage INSTANCE = null;
    static StoreType storeType = StoreType.MMAP;

    public static synchronized ConsumerStorage getConsumerStorage() {
        if(INSTANCE == null){
            // TODO 读取存储策略
            if(storeType == StoreType.RAF) {
                INSTANCE = new ConsumerStorageRAF();
            }else if(storeType == StoreType.MMAP){
                INSTANCE = new ConsumerStorageMMAP();
            }
            INSTANCE.initConsumer();
        }
        return INSTANCE;
    }

    abstract void initConsumer();
    public abstract void getPageBytes(int num, byte[] bytes);

    public List<Integer>[] getIndex(){
        List<Integer>[] res = null;
        try {
            FileInputStream in = new FileInputStream(file_prefix + "index");
            ObjectInputStream i = new ObjectInputStream(in);
            res = (List<Integer>[]) i.readObject();
            i.close();
            in.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return res;
    }
    enum StoreType{
        RAF, MMAP;
    }
}
