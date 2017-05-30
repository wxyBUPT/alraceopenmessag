package io.openmessaging.storage;

import io.openmessaging.Conf;
import io.openmessaging.util.StatusUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiyuanbupt on 5/29/17.
 */
public class ProducerStorageV2 {

    static final int page_size = Conf.PAGE_SIZE;
    static final int file_size = Conf.FILE_SIZE;
    static final int n_block_per_file = file_size / page_size;
    static final String file_prefix = StatusUtil.getFilePath() + File.separator + "mq.";

    int[] curr_file_num = new int[Conf.PRODUCER_COUNT];
    int curr_block[] = new int[Conf.PRODUCER_COUNT];
    RandomAccessFile[] curr_file = new RandomAccessFile[Conf.PRODUCER_COUNT];
    {
        try {
            for (int i = 0, len = curr_file_num.length; i < len; i++) {
                curr_file_num[i] = i ;
                curr_block[i] = -1;
                curr_file[i] = new RandomAccessFile(file_prefix + curr_file_num[i], "rw");
                curr_file[i].setLength((long)file_size);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private final int total_num = Conf.TOTAL_COUNT;
    private final List<Integer>[] index = new List[total_num];
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

    public void storeIndex(){
        storeIndex(index);
    }

    public void storePage(int i, ProducerPage page){
        assert i<Conf.PRODUCER_COUNT;
        byte[] bytes = page.bytes;
        try{
            curr_block[i] ++;
            if(curr_block[i] == n_block_per_file){
                curr_file[i].close();
                curr_file_num[i] = curr_file_num[i] + curr_file_num.length;
                curr_file[i] = new RandomAccessFile(file_prefix + curr_file_num[i], "rw");
                curr_file[i].setLength((long)file_size);
                curr_block[i] = 0;
            }
            curr_file[i].write(bytes, 0, bytes.length);
        }catch (IOException e){
            e.printStackTrace();
        }
        synchronized (index){
            index[page.code].add(curr_file_num[i] * n_block_per_file + curr_block[i]);
        }
    }

    private ProducerStorageV2(){
    }
    static ProducerStorageV2 INSTANCE = null;
    public static synchronized ProducerStorageV2 getInstance(){
        if(INSTANCE == null){
            INSTANCE = new ProducerStorageV2();
        }
        return INSTANCE;
    }
}
