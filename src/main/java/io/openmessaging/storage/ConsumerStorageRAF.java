package io.openmessaging.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiyuanbupt on 5/30/17.
 */
public class ConsumerStorageRAF extends ConsumerStorage{

    // 只有consuemr阶段使用
    List<RandomAccessFile> files = new ArrayList<>(64);
    Object[] readLock ;
    {
        readLock = new Object[256];
        for(int i = 0; i<readLock.length; i++){
            readLock[i] = new Object();
        }
    }

    public ConsumerStorageRAF(){

    }

    @Override
    void initConsumer(){
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


    public void getPageBytes(int num, byte[] bytes) {
        assert bytes.length == page_size;
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
    }
}
