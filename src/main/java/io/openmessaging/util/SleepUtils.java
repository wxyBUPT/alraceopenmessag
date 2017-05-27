package io.openmessaging.util;

import java.util.concurrent.TimeUnit;

/**
 * Created by xiyuanbupt on 5/26/17.
 */

public class SleepUtils {
    public static void sleepSeconds(int n){
        try {
            TimeUnit.SECONDS.sleep(n);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    public static void sleepMillSeconds(int n){
        try{
            TimeUnit.MILLISECONDS.sleep(n);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}