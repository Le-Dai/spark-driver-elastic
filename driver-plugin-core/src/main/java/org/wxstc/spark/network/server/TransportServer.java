package org.wxstc.spark.network.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wxstc.spark.network.protocol.SSDecoder;
import org.wxstc.spark.network.protocol.SSEncoder;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class TransportServer implements Closeable{
    private static final Logger logger = LoggerFactory.getLogger(TransportServer.class);

    private ServerBootstrap bootstrap;
    private ChannelFuture channelFuture;
    private SparkSession spark;
    private int port = -1;

    public TransportServer(String hostToBind, int portToBind, SparkSession spark){
        this.spark = spark;
        init(hostToBind, portToBind);
    }
    private void init(String hostToBind, int portToBind) {

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = bossGroup;

        bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class);
//                .option(ChannelOption.ALLOCATOR, allocator)
//                .childOption(ChannelOption.ALLOCATOR, allocator);


        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                logger.debug("New connection accepted for remote address {}.", ch.remoteAddress());

                ch.pipeline()
                        .addLast("encoder", new SSEncoder())
                        .addLast("decoder", new SSDecoder())
                        .addLast("idleStateHandler", new IdleStateHandler(0, 0, 10000 / 1000))
                        .addLast("handler", new ServerHandler(spark));
            }
        });

        InetSocketAddress address = hostToBind == null ?
                new InetSocketAddress(portToBind): new InetSocketAddress(hostToBind, portToBind);
        channelFuture = bootstrap.bind(address);
//        channelFuture.syncUninterruptibly();
        try {
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        port = portToBind;
        logger.debug("driver plugin server started on port: {}", port);
    }

    @Override
    public void close() throws IOException {
        if (channelFuture != null) {
            // close is a local operation and should finish within milliseconds; timeout just to be safe
            channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
            channelFuture = null;
        }
        if (bootstrap != null && bootstrap.config().group() != null) {
            bootstrap.config().group().shutdownGracefully();
        }
        if (bootstrap != null && bootstrap.config().childGroup() != null) {
            bootstrap.config().childGroup().shutdownGracefully();
        }
        bootstrap = null;
    }
}
