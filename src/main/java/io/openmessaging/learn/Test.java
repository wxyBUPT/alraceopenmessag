package io.openmessaging.learn;

import java.util.Arrays;

/**
 * Created by xiyuanbupt on 5/27/17.
 */
public class Test {
    public static void main(String[] args){
        String a = "0, 1, 0, 3, 102, 111, 111, 0, 3, 98, 97, 114, 0, 2, 0, 3, 49, 46, 48, 63, -16, 0, 0, 0, 0, 0, 0, 0, 2, 49, 76, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 49, 0, 0, 0, 1, 0, 1, 0, 3, 98, 97, 114, 0, 3, 102, 111, 111, 0, 2, 0, 3, 50, 46, 48, 64, 0, 0, 0, 0, 0, 0, 0, 0, 2, 50, 76, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 0, 1, 50, 0, 0, 0, 2, 102, 111, 111, 111, 111, 111, 111, 111, 111, 111";
        String[] key = a.split(",");
        System.out.println(key.length);
        System.out.println(Arrays.toString(key));
    }
}
