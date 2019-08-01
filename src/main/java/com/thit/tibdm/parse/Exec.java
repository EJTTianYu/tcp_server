package com.thit.tibdm.parse;

import com.alibaba.fastjson.JSON;
import com.csvreader.CsvReader;
import com.thit.tibdm.Config;
import com.thit.tibdm.ThreadPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: dongzhiquan  Date: 2019/1/16 Time: 11:27
 */
public class Exec {

    private static final Logger LOGGER= LoggerFactory.getLogger(Exec.class);
    public static void main(String[] args) {
            exex();

        //导出10列车的数据
        //按频率去发送10列车的数据
        //10列车在10个excel里面
        //10列车起10个线程  然后每个线程读一个文件 并发送

    }

    public static void exex(){

        String filepath= Config.I.getDataSendFilePath();
        List<String> fileList = traverseFolder2("D:\\dataanlay\\allqa");


//        for (String path : fileList) {
//            ThreadPoolManager.I.getDataSendThread().submit(new FileSend(path));
//        }


        //每个线程管一个文件
        //

//        for (String path : fileList) {
//            try {
//                CsvReader csvReader=new CsvReader(path);
//                csvReader.readHeaders();
//                while (csvReader.readRecord()){
//                    String rawRecord = csvReader.getRawRecord();
////                    LOGGER.info(csvReader.get(0)+" "+csvReader.get(3)+" "+csvReader.get(4));
////                    LOGGER.info(rawRecord);
//                    String ch=csvReader.get(0);
//                    String time=csvReader.get(3);
//                    String value=csvReader.get(4);
//                    ThreadPoolManager.I.getDataSendThread().submit(new DataSend("1714",Long.parseLong(time),value));
//                    Thread.sleep(30000);
//                }
//
//            } catch (FileNotFoundException e) {
//                LOGGER.error("{}",e);
//            } catch (IOException e) {
//                LOGGER.error("{}",e);
//            } catch (InterruptedException e) {
//                LOGGER.error("{}",e);
//            }
//        }

    }

    public static List<String> traverseFolder2(String path) {
        List<String> list=new ArrayList<>();
        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files!=null && files.length != 0) {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        traverseFolder2(file2.getAbsolutePath());
                    } else {
                        list.add(file2.getAbsolutePath());
                    }
                }
            }
        }
        return list;
    }



}
