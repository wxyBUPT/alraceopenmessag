package io.openmessaging.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiyuanbupt on 5/30/17.
 */
public class ConsumerStorageMMAP extends ConsumerStorage{

    List<FileChannel> channels = new ArrayList<>(64);

    @Override
    void initConsumer() {
        try {
            int cu_fil_nu = -1;
            File file = new File(file_prefix + ++cu_fil_nu);
            while (file.exists()) {
                channels.add(new RandomAccessFile(file, "rw").getChannel());
                file = new File(file_prefix + ++cu_fil_nu);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void getPageBytes(int num, byte[] bytes) {
        assert bytes.length == page_size;
        int file_num = num / n_block_per_file;
        int n_block = num % n_block_per_file;

        FileChannel channel = channels.get(file_num);
        try {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY,
                    (long) n_block * page_size, (long) page_size);
            buffer.get(bytes);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
