package org.wxstc.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

public class SSDecoder extends ReplayingDecoder<Void> {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        ByteBuf byteBuf = in.readBytes(in.readInt());
        Message.Type decode = Message.Type.decode(byteBuf);
        switch (decode){
            case RpcFailure: break;
            case DriverJobRequest: {
                Channel channel = ctx.channel();
                out.add(new DriverRpcRequest(DriverRpcRequest.decode(byteBuf, channel)));
                break;
            }
            case LoadClassRequest: out.add(new LoadClassRequest(LoadClassRequest.decode(byteBuf))); break;
            case NeedClassRequest: out.add(new NeedClassRequest(NeedClassRequest.decode(byteBuf))); break;
            default: break;
        }
    }
}
