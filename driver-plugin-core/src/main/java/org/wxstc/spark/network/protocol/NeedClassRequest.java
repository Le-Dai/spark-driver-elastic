package org.wxstc.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.wxstc.spark.utils.ByteArrayUtils;

import java.util.Map;
import java.util.Optional;

public class NeedClassRequest implements Message {
    public String[] classes = new String[]{};

    public NeedClassRequest(String[] classes){
        this.classes = classes;
    }

    @Override
    public Type type() {
        return Type.NeedClassRequest;
    }

    @Override
    public ByteBuf body() {
        Optional<byte[]> bytes = ByteArrayUtils.objectToBytes(classes);
        ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.buffer(bytes.get().length + 1);
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

    public static String[] decode(ByteBuf buf){
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes, 0, buf.readableBytes());
        Optional<String[]> o = ByteArrayUtils.bytesToObject(bytes);
        return o.get();
    }
}
