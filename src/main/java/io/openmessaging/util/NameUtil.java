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

    public static boolean isTopic(int code){
        return code<topicCount;
    }

    public static String getName(int i){
        return names[i];
    }

    static int topicCount = Conf.TOPIC_COUNT;
    public static int getCode(String name){
        // 如果两个数字位
        int num = 0;
        if(8==name.length()){
            num += 10 * (name.charAt(6) - '0');
            num += (name.charAt(7) - '0');
        }else {
            num += (name.charAt(6) - '0');
        }
        char c = name.charAt(0);
        if(c == 'T'){
            return num;
        }
        else {
            return topicCount + num;
        }
    }

    public static void main(String[] args){
        for(String s:names){
            System.out.println(s);
            System.out.println(getName(getCode(s)));
            System.out.println();
        }
    }
}
