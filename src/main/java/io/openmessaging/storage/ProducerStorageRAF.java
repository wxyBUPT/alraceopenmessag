package io.openmessaging.storage;

import io.openmessaging.Conf;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by xiyuanbupt on 5/30/17.
 */
public class ProducerStorageRAF extends ProducerStorage{

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

    @Override
    public void flush() {

    }

    @Override
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
}
