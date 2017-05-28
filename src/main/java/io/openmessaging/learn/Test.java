package io.openmessaging.learn;

import io.openmessaging.Conf;

import java.util.ArrayDeque;
import java.util.Queue;


/**
 * Created by xiyuanbupt on 5/27/17.
 */
public class Test {
    public static void main(String[] args){
        Queue<Integer> queue = new ArrayDeque<>(16);
        for(int i = 0; i<8; i++){
            queue.add(i);
        }
        System.out.println(queue.size());
        System.out.println(queue.toString());
        for(int i = 0; i<9; i++){
            System.out.println(queue.poll());
        }
    }
}
