package io.openmessaging;

/**
 * Created by xiyuanbupt on 5/26/17.
 */
public class Conf {

    // 磁盘一个页面的大小, 一个页面只存储一个类别的消息 128K 因为考虑到了最大消息的大小
    public final static int PAGE_SIZE = 200 * 1024;
    public final static int PAGE_CACHE_SIZE_PER_PRODUCER = 10;
    // DefaultBytesMessage{headers=DefaultKeyValue{int_values=null, long_values=null, double_values=null, string_values={Queue=QUEUE_0, MessageId=z2x7uxo0ljfgj}}, properties=DefaultKeyValue{int_values=null, long_values=null, double_values=null, string_values={PRO_OFFSET=PRODUCER3_1347, fgj5zcw=uvyherb, fgjzw9f=uvyeblu, fgjmdq9=uvy1s5l}}}
    public final static int PAGE_SAFE_EDAGE = 128;
    // 一个文件128M, 能够保存1024个page
    public final static int FILE_SIZE = 200 * 1024 * 1024;


    // 数量
    public final static int TOPIC_COUNT = 90;
    public final static int QUEUE_COUNT = 10;
    public final static int PRODUCER_COUNT = 10;
    public final static int TOTAL_COUNT = TOPIC_COUNT + QUEUE_COUNT;

    // 共有多少pull consumer
    public final static int CONSUMER_THREAD_COUNT = 10;
    // 每个pull consumer cache 消息的block块数
    public final static int CONSUMER_CACHE_BLOCK_SIZE = 100;
    // Consumer 解码线程的个数
    public final static int CONSUMER_DECODE_THREAD_COUNT = 4;
    // 每个解码线程缓存page数目
    public final static int CONSUMER_DECODE_CACHE_BLOCK_COUNT = 100;
}
