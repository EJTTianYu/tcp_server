package com.thit.tibdm.parse;

import com.thit.tibdm.HexUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: dongzhiquan  Date: 2019/1/15 Time: 10:26
 */
public class DataSend implements Runnable{

    private final static Logger LOGGER= LoggerFactory.getLogger(DataSend.class);
    
    private Channel instance;

    private String use_rawdata;

    private String ch;

    private long time;

    public DataSend(String ch, long time, String use_rawdata) {
        this.ch = ch;
        this.time = System.currentTimeMillis();
        this.use_rawdata = use_rawdata;
        this.raw_data=Unpooled.buffer();
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getCh() {
        return ch;
    }

    public void setCh(String ch) {
        this.ch = ch;
    }

    public String getUse_rawdata() {
        return use_rawdata;
    }

    public void setUse_rawdata(String use_rawdata) {
        this.use_rawdata = use_rawdata;
    }

    public ByteBuf getRaw_data() {
        return raw_data;
    }

    public void setRaw_data(ByteBuf raw_data) {
        this.raw_data = raw_data;
    }

    //导出数据 到本地磁盘

    //读文件

    //数据进行车号改装

    //数据循环发送

    //构造rawdata

    private ByteBuf raw_data;



    


    public  ByteBuf getraw_data(){

        //有效数据
        byte[] bytes = HexUtil.hexString2Bytes(use_rawdata);

        String date = getDate(time);

        //消息头
        raw_data.writeByte(0xAA);
        raw_data.writeByte(0xAB);
        raw_data.writeByte(0xAC);
        //消息类型
        raw_data.writeByte(0x01);
        //消息id??
        raw_data.writeInt(11);

        //通讯协议类型
        raw_data.writeByte(2);
        //通讯协议版本号
        raw_data.writeByte(0x10);
        //数据长度
        raw_data.writeInt(bytes.length+32);
        //城市id?
        raw_data.writeByte(1);
        //线路id?
        raw_data.writeByte(17);
        //编组id
        raw_data.writeShort(Integer.parseInt(ch));
        //设备id
        raw_data.writeByte(1);
        //年月日时分秒
        raw_data.writeByte(Integer.parseInt(date.substring(2, 4)));
        raw_data.writeByte(Integer.parseInt(date.substring(4, 6)));
        raw_data.writeByte(Integer.parseInt(date.substring(6, 8)));
        raw_data.writeByte(Integer.parseInt(date.substring(8, 10)));
        raw_data.writeByte(Integer.parseInt(date.substring(10, 12)));
        raw_data.writeByte(Integer.parseInt(date.substring(12, 14)));
        //毫秒
        raw_data.writeShort(Integer.parseInt(date.substring(14, 17)));

        //预留??
        raw_data.writeByte(1);
        raw_data.writeByte(1);
        raw_data.writeByte(1);
        raw_data.writeByte(1);
        raw_data.writeByte(1);
        raw_data.writeByte(1);
        raw_data.writeByte(1);
        raw_data.writeByte(1);
        //有效数据
        raw_data.writeBytes(bytes);
        //crc校验
        raw_data.writeByte(1);
        raw_data.writeByte(1);
        //消息尾
        raw_data.writeByte(0xBA);
        raw_data.writeByte(0xBB);
        raw_data.writeByte(0xBC);

        return raw_data;
    }



    @Override
    public void run() {
        raw_data=getraw_data();
        ClientConnect connect1 = ClientConnectPool.I.getConnect(ch);
        Channel connect2 = connect1.getConnect();
        connect2.writeAndFlush(raw_data);
        LOGGER.info("发送数据成功");

    }


    public static String getDate(long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmssSSS");
        String format = sdf.format(new Date(time));
        return format;
    }
}
