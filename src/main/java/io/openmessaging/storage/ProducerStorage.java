package io.openmessaging.storage;

import io.openmessaging.Conf;
import io.openmessaging.util.StatusUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiyuanbupt on 5/29/17.
 */
public abstract class ProducerStorage {

    static final int page_size = Conf.PAGE_SIZE;
    static final int file_size = Conf.FILE_SIZE;
    static final int n_block_per_file = file_size / page_size;
    static final String file_prefix = StatusUtil.getFilePath() + File.separator + "mq.";

    int[] curr_file_num = new int[Conf.PRODUCER_COUNT];
    int curr_block[] = new int[Conf.PRODUCER_COUNT];

    private final int total_num = Conf.TOTAL_COUNT;
    final List<Integer>[] index = new List[total_num];
    {
        for(int i = 0;i <total_num; i++){
            index[i] = new ArrayList<>(32);
        }
    }

    private void storeIndex(List<Integer>[] index){
        try {
            FileOutputStream out = new FileOutputStream(file_prefix + "index");
            ObjectOutputStream o = new ObjectOutputStream(out);
            o.writeObject(index);
            o.close();
            out.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void finish(){
        storeIndex(index);
    }

    public abstract void storePage(int i, ProducerPage page);
    public abstract void flush();

    ProducerStorage(){
    }
    static ProducerStorage INSTANCE = null;
    static PStoreType storeType = PStoreType.MMAP;
    public static synchronized ProducerStorage getInstance(){
        if(INSTANCE == null){
            if(storeType == PStoreType.RAF) {
                INSTANCE = new ProducerStorageRAF();
            }else if(storeType == PStoreType.MMAP){
                INSTANCE = new ProducerStorageMMAP();
            }
        }
        return INSTANCE;
    }

    enum PStoreType{
        RAF, MMAP
    }
}
