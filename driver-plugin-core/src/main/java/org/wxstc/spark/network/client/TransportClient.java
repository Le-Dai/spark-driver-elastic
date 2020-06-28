package org.wxstc.spark.network.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wxstc.spark.network.protocol.DriverRpcRequest;
import org.wxstc.spark.network.protocol.LoadClassRequest;
import org.wxstc.spark.network.protocol.SSDecoder;
import org.wxstc.spark.network.protocol.SSEncoder;
import org.wxstc.spark.utils.ByteArrayUtils;
import org.wxstc.spark.utils.ClassDependenciesUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TransportClient implements Closeable{
    private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

    private ServerBootstrap bootstrap;
    private ChannelFuture channelFuture;
    private int port = -1;

    final AtomicReference<Channel> channelRef = new AtomicReference<>();
    public TransportClient(SocketAddress address){
        init(address);
    }
    private void init(SocketAddress address) {

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = bossGroup;
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                // Disable Nagle's Algorithm since we don't want packets to wait
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
//                .option(ChannelOption.ALLOCATOR, pooledAllocator);

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                ch.pipeline()
                        .addLast("encoder", new SSEncoder())
                        .addLast("decoder", new SSDecoder())
                        .addLast("idleStateHandler", new IdleStateHandler(0, 0, 10000 / 1000))
                        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
                        // would require more logic to guarantee if this were not part of the same event loop.
                        .addLast("handler", new ClientHandler());
                channelRef.set(ch);
            }
        });

        // Connect to the remote server
        long preConnect = System.nanoTime();
        ChannelFuture cf = bootstrap.connect(address);
        try {
            if (!cf.await(10000)) {
                throw new IOException(
                        String.format("Connecting to %s timed out (%s ms)", address, 10000));
            } else if (cf.cause() != null) {
                throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    public void sendAction(DriverRpcRequest driverRpcRequest){
        try {
            Map<String, byte[]> dependenciesBinary = ClassDependenciesUtils.getDependenciesBinary(driverRpcRequest.getRunnable().getClass());
            channelRef.get().writeAndFlush(new LoadClassRequest(dependenciesBinary)).sync();
            channelRef.get().writeAndFlush(driverRpcRequest).sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public AtomicReference<Channel> getChannelRef() {
        return channelRef;
    }
}
