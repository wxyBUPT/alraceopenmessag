package io.openmessaging.storage;

import io.openmessaging.Conf;
import io.openmessaging.util.StatusUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by xiyuanbupt on 5/26/17.
 * 约定
 * 1. 需要与上层应用协商好pagesize
 * 2. 只负责高效的存储 byte[] 形式的page, 并返回page标号, 以及通过标号取出byte[] 形式的page
 */
public class Storage {

    static final int page_size = Conf.PAGE_SIZE;
    static final int file_size = Conf.FILE_SIZE;
    static final int n_block_per_file = file_size / page_size;
    static final String file_prefix = StatusUtil.getFilePath() + File.separator + "mq.";

    // 只有consuemr阶段使用
    List<RandomAccessFile> files = new ArrayList<>(64);
    Object[] readLock ;
    {
        readLock = new Object[256];
        for(int i = 0; i<readLock.length; i++){
            readLock[i] = new Object();
        }
    }

    // 只有producer阶段使用
    int curr_file_num = -1;
    RandomAccessFile currFile;
    int curr_block = -1;

    private Storage(){

    }

    static Storage INSTANCE = null;
    public static synchronized Storage getProducerStorage(){
        if(INSTANCE == null){
            INSTANCE = new Storage();
            INSTANCE.initProducer();
        }
        return INSTANCE;
    }

    private void initProducer(){
        try {
            currFile = new RandomAccessFile(file_prefix + ++curr_file_num, "rw");
            currFile.setLength((long) file_size);
            files.add(currFile);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static synchronized Storage getConsumerStorage() {
        // TODO 从存储中读出打开文件的标号, 并打开所有所有的文件
        if(INSTANCE == null){
            INSTANCE = new Storage();
            INSTANCE.initConsumer();
        }
        return INSTANCE;
    }

    private void initConsumer(){
        try {
            int cu_fil_nu = -1;
            File file = new File(file_prefix + ++cu_fil_nu);
            while (file.exists()) {
                files.add(new RandomAccessFile(file, "rw"));
                file = new File(file_prefix + ++cu_fil_nu);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public synchronized int storePageBytes(byte[] bytes){
        if(bytes.length != page_size){
            throw new RuntimeException("Only support " + page_size + "byte block");
        }
        try {
            // 文件已经满了
            curr_block++;
            if (curr_block == n_block_per_file) {
                currFile.close();
                currFile = new RandomAccessFile(file_prefix + ++curr_file_num, "rw");
                currFile.setLength((long) file_size);
                curr_block = 0;
            }

            currFile.write(bytes);
            currFile.getFD().sync();
        }catch (IOException e){
            e.printStackTrace();
        }
        return curr_file_num * n_block_per_file + curr_block;
    }

    public byte[] getPageBytes(int num) {
        byte[] bytes = new byte[page_size];
        int file_num = num / n_block_per_file;
        int n_block = num % n_block_per_file;
        RandomAccessFile raf = files.get(file_num);
        try {
            synchronized (readLock[file_num]) {
                raf.seek((long) n_block * page_size);
                raf.read(bytes);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return bytes;
    }

    public void storeIndex(List<Integer>[] index){
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
}
