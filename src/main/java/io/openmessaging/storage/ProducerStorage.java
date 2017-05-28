package io.openmessaging.storage;

import io.openmessaging.Conf;
import io.openmessaging.demo.DefaultProducer;
import io.openmessaging.util.StatusUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by xiyuanbupt on 5/27/17.
 */
public class ProducerStorage {
    Logger logger = LoggerFactory.getLogger(ProducerStorage.class);

    static final int page_size = Conf.PAGE_SIZE;
    static final int file_size = Conf.FILE_SIZE;
    static final int n_block_per_file = file_size / page_size;
    static final String file_prefix = StatusUtil.getFilePath() + File.separator + "mq.";
    private BlockingQueue<ProducerPage>[] pagesCache;
    private BlockingQueue<ProducerPage> pagesPool;
    // 只有producer阶段使用
    int curr_file_num = -1;
    RandomAccessFile currFile;
    int curr_block = -1;

    private final int total_num = Conf.TOTAL_COUNT;
    private final List<Integer>[] index = new List[total_num];
    {
        for(int i =0; i<total_num; i++){
            index[i] = new ArrayList<>(32);
        }
    }

    private ProducerStorage(){
    }
    static ProducerStorage INSTANCE = null;
    public static synchronized ProducerStorage getInstance(){
        if(INSTANCE == null){
            INSTANCE = new ProducerStorage();
            INSTANCE.init();
        }
        return INSTANCE;
    }

    private void init(){
        try {
            currFile = new RandomAccessFile(file_prefix + ++curr_file_num, "rw");
            currFile.setLength((long) file_size);
            pagesCache = new BlockingQueue[Conf.PRODUCER_COUNT];
            for(int i = 0; i<pagesCache.length; i++){
                pagesCache[i] = new LinkedBlockingQueue<>(Conf.PAGE_CACHE_SIZE_PER_PRODUCER);
            }
            pagesPool = new ArrayBlockingQueue<ProducerPage>(Conf.PAGE_CACHE_SIZE_PER_PRODUCER* Conf.PRODUCER_COUNT);
            new Thread(new IOThread(), "ProducerIOThread").start();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    // 将消息放入缓存队列, 如果队列满会一直阻塞
    public void putPage(int p_id, ProducerPage page){
        try {
            pagesCache[p_id].put(page);
        }catch (InterruptedException e){
            logger.info("Should not interrupt this function !");
        }
    }

    public ProducerPage pollPageFromPool(){
        return pagesPool.poll();
    }

    class IOThread implements Runnable{
        @Override
        public void run() {
            try {
                while (true) {
                    ProducerPage page = null;
                    for(BlockingQueue<ProducerPage> cache:pagesCache){
                        page = cache.poll();
                        if(page != null)break;
                    }
                    if(page != null){
                        int block = storePageBytes(page.bytes);
                        index[page.code].add(block);
                        boolean offered = pagesPool.offer(page);
                        continue;
                    }
                    // TODO 如果为null, 则需要判断生产者是否结束生产
                    if(DefaultProducer.threadCounter.get() == 0){
                        break;
                    }
                }
                // 通知所有生茶这可以退出
                storeIndex(index);
                currFile.close();
                DefaultProducer.ioFinish.countDown();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
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

    public int storePageBytes(byte[] bytes){
        try {
            // 文件已经满了
            curr_block++;
            if (curr_block == n_block_per_file) {
                currFile.close();
                currFile = new RandomAccessFile(file_prefix + ++curr_file_num, "rw");
                currFile.setLength((long) file_size);
                curr_block = 0;
            }

            currFile.write(bytes, 0, bytes.length);
        }catch (IOException e){
            e.printStackTrace();
        }
        return curr_file_num * n_block_per_file + curr_block;
    }
}
