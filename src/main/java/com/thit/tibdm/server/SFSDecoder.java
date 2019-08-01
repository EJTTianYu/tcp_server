package com.thit.tibdm.server;

import com.thit.tibdm.HexUtil;
import com.thit.tibdm.parse.ContentDecode;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 解码类
 */
public class SFSDecoder extends ByteToMessageDecoder {
    /**
     * 日志类
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SFSDecoder.class);
    /**
     * 设定的最小长度
     */
    private static final int MIN_LEN = 40;
    /**
     * 设置的最大长度
     */
    private static final int MAXPACKET_SIZE = 8096;

    /**
     * 解码类
     *
     * @param ctx 连接上下文
     * @param in  输入
     * @param out 输出
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        decodeFrames(in, out);
    }

    /**
     * 将bytebuf拆包
     *
     * @param in
     * @param out
     */
    private void decodeFrames(ByteBuf in, List<Object> out) {
        //这个包的长度
        int length = in.readableBytes();
        if (length >= MIN_LEN) {
            //1.记录当前读取位置位置.如果读取到非完整的frame,要恢复到该位置,便于下次读取
            in.markReaderIndex();
            SFSPacket packet = decodeFrame(in);
            if (packet != null) {
                out.add(packet);
            } else {
                //2.读取到不完整的frame,恢复到最近一次正常读取的位置,便于下次读取
                in.resetReaderIndex();
            }
        }
    }

    /**
     * 解析包头包尾等
     *
     * @param in
     * @return
     */
    private SFSPacket decodeFrame(ByteBuf in) {
        //可读字节长度
        int readableBytes = in.readableBytes();
        //获取数据包的长度字段验证
        long bodylenth = in.getUnsignedInt(10);
        //命令，那种数据包
        byte cmd = (byte) in.getUnsignedByte(3);
        long packetLength = bodylenth + 8;
        long ch = in.getUnsignedShort(16);
        if (packetLength > MAXPACKET_SIZE || packetLength < readableBytes) {
            byte[] bytes = new byte[readableBytes];
            LOGGER.error("车辆信息{}", ch);
            LOGGER.error("数据包的长度：{},可读数据包长度：{}", packetLength, readableBytes);
            in.readBytes(bytes);
            LOGGER.error("不正确的数据包：{}", HexUtil.getFrame(bytes, " "));
            throw new RuntimeException("数据长度异常的原始数据包");
        } else if (packetLength > readableBytes) {
            return null;
        } else if (!ContentDecode.I.isRecord(ch)) {
//        } else if (false){
            throw new RuntimeException("系统未配置车号" + ch);
        } else {
            return SFSPacket.decodeSFSPacket(new SFSPacket((int) bodylenth, cmd), in);
        }
    }
}
