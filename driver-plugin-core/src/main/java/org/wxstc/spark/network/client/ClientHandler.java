package org.wxstc.spark.network.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.wxstc.spark.network.protocol.LoadClassRequest;
import org.wxstc.spark.network.protocol.Message;
import org.wxstc.spark.network.protocol.NeedClassRequest;
import org.wxstc.spark.utils.ByteArrayUtils;

import java.util.HashMap;


public class ClientHandler extends SimpleChannelInboundHandler<Message> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        if(msg instanceof NeedClassRequest){
            String[] classes = ((NeedClassRequest) msg).classes;
            HashMap<String, byte[]> clazzs = new HashMap<>();
            for (String clazz:classes) {
                byte[] classBytes = ByteArrayUtils.getClassBytes(Class.forName(clazz));
                clazzs.put(clazz, classBytes);
            }
            ctx.writeAndFlush(new LoadClassRequest(clazzs));
        }else if(msg instanceof LoadClassRequest){
        }
        System.out.println(msg);
    }
}
