package io.openmessaging.util;

import io.openmessaging.Conf;

/**
 * Created by xiyuanbupt on 5/26/17.
 */
public class NameUtil {

    static String[] names = new String[Conf.TOTAL_COUNT];
    static {
        for(int i = 0; i<Conf.TOPIC_COUNT; i++){
            names[i] = "TOPIC_" + i;
        }
        for(int i = 0; i<Conf.QUEUE_COUNT; i++){
            names[Conf.TOPIC_COUNT + i] = "QUEUE_" + i;
        }
    }

    public static String getName(int i){
        return names[i];
    }

    static int topicCount = Conf.TOPIC_COUNT;
    public static int getCode(String name){
        int i = name.indexOf("_");
        int num = Integer.parseInt(name.substring(i+1));
        char c = name.charAt(0);
        if(c == 'T'){
            return num;
        }
        else {
            return topicCount + num;
        }
    }
}
