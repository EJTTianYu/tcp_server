package com.thit.tibdm;


import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * @author: dongzhiquan  Date: 2018/10/12 Time: 10:31
 */
public class FileHandle {
    private static final Logger logger = LoggerFactory.getLogger(FileHandle.class);
    private final static String fix = ".txt";

    public static String getFix() {
        return fix;
    }

    /**
     * @param msg      字符
     * @param filePath 文件路径
     */
    public static void save2File(String msg, String filePath) {
        File file = FileUtils.getFile(filePath);
        List<String> list = new ArrayList<>();
        list.add(msg);
        try {
            FileUtils.writeLines(file, "UTF-8", list, true);
        } catch (IOException e) {
            logger.error("{} 消息写入文件失败!", msg);
            logger.error("{}", e);
        }
    }

    /**
     * 根据时间戳获取文件名
     *
     * @param time
     * @return
     */
    public static String getFilename(long time) {
        String name = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        name = sdf.format(new Date(time));
        return name;
    }

    /**
     * 获取项目路径
     *
     * @return
     */
    public static String getPath() {
        return System.getProperty("user.dir") + File.separator + "data" + File.separator;
    }

}
