package io.openmessaging.storage;

import io.openmessaging.Conf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by xiyuanbupt on 5/30/17.
 */
public class ProducerStorageMMAP extends ProducerStorage{

    MappedByteBuffer[] buffers = new MappedByteBuffer[Conf.PRODUCER_COUNT];
    FileChannel[] channels = new FileChannel[Conf.PRODUCER_COUNT];
    RandomAccessFile[] files = new RandomAccessFile[Conf.PRODUCER_COUNT];
    {
        try {
            for (int i = 0; i < curr_file_num.length; i++) {
                curr_file_num[i] = i;
                curr_block[i] = -1;
                RandomAccessFile file = new RandomAccessFile(file_prefix + curr_file_num[i], "rw");
                files[i] = file;
                file.setLength((long) file_size);
                FileChannel channel = file.getChannel();
                channels[i] = channel;
                buffers[i] = channel.map(FileChannel.MapMode.READ_WRITE, 0L, (long) file_size);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    int[] in_mem_block_count = new int[Conf.PRODUCER_COUNT];
    {
        for(int i = 0; i<in_mem_block_count.length; i++){
            in_mem_block_count[i] = -1;
        }
    }

    @Override
    public void storePage(int i, ProducerPage page) {
        assert i<Conf.PRODUCER_COUNT;
        byte[] bytes = page.bytes;

        try {
            curr_block[i]++;
            if (curr_block[i] == n_block_per_file) {
                buffers[i].force();
                in_mem_block_count[i] = -1;
                channels[i].close();
                files[i].close();
                curr_file_num[i] = curr_file_num[i] + curr_file_num.length;
                RandomAccessFile file = new RandomAccessFile(file_prefix + curr_file_num[i], "rw");
                files[i] = file;
                file.setLength((long) file_size);
                FileChannel channel = file.getChannel();
                channels[i] = channel;
                buffers[i] = channel.map(FileChannel.MapMode.READ_WRITE, 0L, (long) file_size);
                curr_block[i] = 0;
            }
            buffers[i].put(bytes);
            // 32块刷一次盘
            if(++in_mem_block_count[i]==32){
                buffers[i].force();
                in_mem_block_count[i] = -1;
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        synchronized (index){
            index[page.code].add(curr_file_num[i] * n_block_per_file + curr_block[i]);
        }
    }

    @Override
    public void flush() {

    }
}
