package io.openmessaging.learn;

import io.openmessaging.Conf;

import java.util.Arrays;

/**
 * Created by xiyuanbupt on 5/27/17.
 */
public class Test {
    public static void main(String[] args){
        int hashbits = Conf.CONSUMER_DECODE_THREAD_COUNT - 1;
        for(int i = 0; i<100; i++){
            System.out.println(i & hashbits);
        }
    }
}
