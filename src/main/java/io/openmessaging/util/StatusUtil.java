package io.openmessaging.util;

import java.io.File;
import java.io.IOException;

/**
 * Created by xiyuanbupt on 5/26/17.
 */
public class StatusUtil {

    // 判断当前进程是否是producer进程
    transient private static Boolean producering = true;
    private static String filePath;
    private static transient boolean inited = false;

    public synchronized static void init(String filPath, boolean isProducer) throws IOException {
        if(inited)return;
        inited = true;
        filePath = filPath;
        if(isProducer){
            // 在下面应该执行删除所有文件的操作
            producering = true;
        }else {
            producering = false;
        }
    }

    public static boolean isproducer(){
        return producering;
    }

    public static String getFilePath() {
        return filePath;
    }
}
