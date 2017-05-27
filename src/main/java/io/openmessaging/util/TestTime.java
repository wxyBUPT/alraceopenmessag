package io.openmessaging.util;

/**
 * Created by xiyuanbupt on 5/26/17.
 */
public abstract class TestTime {
    int testcount;

    public TestTime(int testcount){
        this.testcount = testcount;
    }

    public long getTime(){
        long start = System.currentTimeMillis();
        runtest();
        return System.currentTimeMillis() - start;
    }

    abstract void runtest();

}
