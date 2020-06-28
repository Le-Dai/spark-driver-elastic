package org.wxstc.spark.network.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.spark.sql.SparkSession;
import org.wxstc.spark.execute.ExecutorThreadPool;
import org.wxstc.spark.network.protocol.DriverRpcRequest;
import org.wxstc.spark.network.protocol.LoadClassRequest;
import org.wxstc.spark.network.protocol.Message;
import org.wxstc.spark.plugins.SparkJob;
import org.wxstc.spark.plugins.SparkJobRunnable;
import org.wxstc.spark.utils.MemoryClassLoader;

import java.rmi.RemoteException;
import java.util.HashMap;

public class ServerHandler extends SimpleChannelInboundHandler<Message> {
    private SparkSession spark;
    public static MemoryClassLoader loader = new MemoryClassLoader(new HashMap<>());
    public ServerHandler(SparkSession spark){
        this.spark = spark;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        if(msg instanceof DriverRpcRequest){
            ExecutorThreadPool.INSTANCE.executeJob(() -> {
                try {
                    SparkJob job = (SparkJob) ((DriverRpcRequest) msg).getRunnable();
                    job.initEnv(spark, null);
                    job.run();
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            });
        }else if(msg instanceof LoadClassRequest){
            loader.addOrUpdateAllClass(((LoadClassRequest) msg).classes);
        }
    }
}
