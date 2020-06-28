package org.wxstc.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.wxstc.spark.network.server.ServerHandler;
import org.wxstc.spark.plugins.SparkJobRunnable;
import org.wxstc.spark.utils.ByteArrayUtils;
import org.wxstc.spark.utils.MemoryClassLoader;

import java.io.*;
import java.util.Optional;

public class DriverRpcRequest implements Message{
    SparkJobRunnable runnable;
    byte[] bytes = null;
    byte[] classBytes = null;
    ByteBuf byteBuf = null;

    private static MemoryClassLoader loader = ServerHandler.loader;
    public DriverRpcRequest(SparkJobRunnable runnable) throws IOException {
        this.runnable = runnable;
        bytes = ByteArrayUtils.objectToBytes(runnable).get();
        classBytes = ByteArrayUtils.objectToBytes(runnable.getClass()).get();
        Thread.currentThread().setContextClassLoader(loader);
    }

    public DriverRpcRequest(ByteBuf byteBuf) throws IOException {
        this.byteBuf = byteBuf;
    }

    @Override
    public Type type() {
        return Type.DriverJobRequest;
    }

    @Override
    public ByteBuf body() {
        ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.buffer(bytes.length);
        buffer.writeByte(type().id());
        buffer.writeBytes(bytes);
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

    public static SparkJobRunnable decode(ByteBuf buf, Channel channel){
        DriverRpcRequest.class.getClassLoader();
        int error = 0;
        ByteBuf buffer = new UnpooledHeapByteBuf(UnpooledByteBufAllocator.DEFAULT, buf.readableBytes(), buf.readableBytes());
        buf.readBytes(buffer, buf.readableBytes());


        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        SparkJobRunnable obj = null;

        Optional<Object> o = ByteArrayUtils.bytesToObjectMemory(buffer.array());
        obj = (SparkJobRunnable) o.get();
//        try {
//            bis = new ByteArrayInputStream (buffer.array());
//            ois = new ObjectInputStream(bis);
//            try {
//                obj = (SparkJobRunnable) ois.readObject();
//                return obj;
//            } catch (ClassNotFoundException e2){
//                error ++;
//                if(error >= 10) return obj;
//                String clazzName = e2.getMessage();
//                //memory classloader dose not exits classes
//                channel.writeAndFlush(new NeedClassRequest(new String[]{clazzName})).sync();
//            }catch (NoClassDefFoundError e3){
//                error ++;
//                if(error >= 10) return obj;
//                String clazzName = e3.getMessage().replaceAll("/", ".");
//                //memory classloader dose not exits classes
//                channel.writeAndFlush(new NeedClassRequest(new String[]{clazzName})).sync();
//            }
//            return obj;
//        } catch (Exception e){
//            e.printStackTrace();
//        } finally {
//            try {
//                ois.close();
//                bis.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
        return obj;
    }

    public SparkJobRunnable getRunnable() {
        return runnable;
    }

    public void setRunnable(SparkJobRunnable runnable) {
        this.runnable = runnable;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public ByteBuf getByteBuf() {
        return byteBuf;
    }

    public void setByteBuf(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }
}
