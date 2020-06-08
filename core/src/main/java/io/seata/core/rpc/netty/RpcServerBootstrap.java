/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.core.rpc.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.seata.common.XID;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.core.rpc.RemotingBootstrap;
import io.seata.core.rpc.netty.v1.ProtocolV1Decoder;
import io.seata.core.rpc.netty.v1.ProtocolV1Encoder;
import io.seata.discovery.registry.RegistryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToDoubleBiFunction;

/**
 * Rpc server bootstrap.
 *  TODO: RPC Server的启动类
 * @author zhangchenghui.dev@gmail.com
 * @since 1.1.0
 */
public class RpcServerBootstrap implements RemotingBootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcServerBootstrap.class);

    /**
     * TODO: 创建一个serverBootStrap, 用于配置server端配置
     */
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    /**
     * work线程组
     */
    private final EventLoopGroup eventLoopGroupWorker;
    /**
     * boss线程组
     */
    private final EventLoopGroup eventLoopGroupBoss;
    /**
     * 引用配置类
     */
    private final NettyServerConfig nettyServerConfig;
    /**
     * 需要添加的channelHandler
     */
    private ChannelHandler[] channelHandlers;
    /**
     * 服务端监听的端口号
     */
    private int listenPort;
    /**
     * 原子类，用于判断是否初始化
     */
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    public RpcServerBootstrap(NettyServerConfig nettyServerConfig) {
        // TODO: 把nettyServerConfig保存起来，使用this.nettyServerConfig进行引用
        this.nettyServerConfig = nettyServerConfig;
        // TODO: 如果开启了epoll的方式，使用EpollEventLoopGroup
        if (NettyServerConfig.enableEpoll()) {
            this.eventLoopGroupBoss = new EpollEventLoopGroup(nettyServerConfig.getBossThreadSize(),
                new NamedThreadFactory(nettyServerConfig.getBossThreadPrefix(), nettyServerConfig.getBossThreadSize()));
            this.eventLoopGroupWorker = new EpollEventLoopGroup(nettyServerConfig.getServerWorkerThreads(),
                new NamedThreadFactory(nettyServerConfig.getWorkerThreadPrefix(),
                    nettyServerConfig.getServerWorkerThreads()));
        } else {
            // TODO: 否则使用NIO的方式
            this.eventLoopGroupBoss = new NioEventLoopGroup(nettyServerConfig.getBossThreadSize(),
                new NamedThreadFactory(nettyServerConfig.getBossThreadPrefix(), nettyServerConfig.getBossThreadSize()));
            this.eventLoopGroupWorker = new NioEventLoopGroup(nettyServerConfig.getServerWorkerThreads(),
                new NamedThreadFactory(nettyServerConfig.getWorkerThreadPrefix(),
                    nettyServerConfig.getServerWorkerThreads()));
        }

        // init listenPort in constructor so that getListenPort() will always get the exact port
        // TODO: 设置监听的端口号
        setListenPort(nettyServerConfig.getDefaultListenPort());
    }

    /**
     * Sets channel handlers.
     *
     * @param handlers the handlers
     */
    protected void setChannelHandlers(final ChannelHandler... handlers) {
        if (null != handlers) {
            channelHandlers = handlers;
        }
    }

    /**
     * Add channel pipeline last.
     *
     * @param channel  the channel
     * @param handlers the handlers
     */
    private void addChannelPipelineLast(Channel channel, ChannelHandler... handlers) {
        if (null != channel && null != handlers) {
            channel.pipeline().addLast(handlers);
        }
    }

    /**
     * Sets listen port.
     *
     * @param listenPort the listen port
     */
    public void setListenPort(int listenPort) {

        if (listenPort <= 0) {
            throw new IllegalArgumentException("listen port: " + listenPort + " is invalid!");
        }
        this.listenPort = listenPort;
    }

    /**
     * Gets listen port.
     *
     * @return the listen port
     */
    public int getListenPort() {
        return listenPort;
    }

    /**
     * 此处核心 启动seata server
     */
    @Override
    public void start() {
        // 设置两个线程组
        this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupWorker)
                // server channel class
            .channel(NettyServerConfig.SERVER_CHANNEL_CLAZZ)
                // 当前连接的配置
            .option(ChannelOption.SO_BACKLOG, nettyServerConfig.getSoBackLogSize())
            .option(ChannelOption.SO_REUSEADDR, true)
                // 子连接的配置
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSendBufSize())
            .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketResvBufSize())
            .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                new WriteBufferWaterMark(nettyServerConfig.getWriteBufferLowWaterMark(),
                    nettyServerConfig.getWriteBufferHighWaterMark()))
                // 设置监听端口号
            .localAddress(new InetSocketAddress(listenPort))
                // 开始配置handler
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    // TODO: 添加一个监听读写空闲的
                    ch.pipeline().addLast(new IdleStateHandler(nettyServerConfig.getChannelMaxReadIdleSeconds(), 0, 0))
                            // TODO: 编码器，解码器
                        .addLast(new ProtocolV1Decoder())
                        .addLast(new ProtocolV1Encoder());
                    // 如果配置的channelHandlers不为空，则把channelHandlers全搞进去
                    if (null != channelHandlers) {
                        addChannelPipelineLast(ch, channelHandlers);
                    }

                }
            });

        try {
            // TODO: 大功告成，开始监听端口，启动服务
            ChannelFuture future = this.serverBootstrap.bind(listenPort).sync();
            LOGGER.info("Server started ... ");
            // TODO: 开始注册服务了，如果配置的eureka，则注册到eureka上面去
            RegistryFactory.getInstance().register(new InetSocketAddress(XID.getIpAddress(), XID.getPort()));
            // TODO: 设置服务为初始化完成
            initialized.set(true);
            future.channel().closeFuture().sync();
        } catch (Exception exx) {
            throw new RuntimeException(exx);
        }

    }

    @Override
    public void shutdown() {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Shutting server down. ");
            }
            // TODO: 如果已经开启了服务
            if (initialized.get()) {
                // TODO: 这里去相对应的服务注册中心去卸载服务
                RegistryFactory.getInstance().unregister(new InetSocketAddress(XID.getIpAddress(), XID.getPort()));
                // TODO: 之后进行关闭，然后等待几秒
                RegistryFactory.getInstance().close();
                //wait a few seconds for server transport
                TimeUnit.SECONDS.sleep(nettyServerConfig.getServerShutdownWaitTime());
            }
            // TODO: 优雅的关闭线程组
            this.eventLoopGroupBoss.shutdownGracefully();
            this.eventLoopGroupWorker.shutdownGracefully();
        } catch (Exception exx) {
            LOGGER.error(exx.getMessage());
        }
    }
}
