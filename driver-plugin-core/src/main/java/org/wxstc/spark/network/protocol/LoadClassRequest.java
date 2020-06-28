package org.wxstc.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import org.wxstc.spark.plugins.SparkJobRunnable;
import org.wxstc.spark.utils.ByteArrayUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LoadClassRequest implements Message {
    public Map<String, byte[]> classes = new HashMap<>();
    public LoadClassRequest(){
    }

    public LoadClassRequest(Map<String, byte[]> classes){
        this.classes = classes;
    }

    @Override
    public Type type() {
        return Type.LoadClassRequest;
    }

    @Override
    public ByteBuf body() {
        Optional<byte[]> bytes = ByteArrayUtils.objectToBytes(classes);
        ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.buffer(bytes.get().length);
        buffer.writeByte(type().id());
        buffer.writeBytes(bytes.get());
        return buffer;
    }

    @Override
    public boolean isBodyInFrame() {
        return true;
    }

    @Override
    public int encodedLength() {
        return 0;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeBytes(body());
    }

    public static Map<String, byte[]> decode(ByteBuf buf){
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes, 0, buf.readableBytes());
        Optional<Map<String, byte[]>> o = ByteArrayUtils.bytesToObject(bytes);
        return o.get();
    }
}
