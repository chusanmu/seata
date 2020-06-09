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

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.seata.common.exception.FrameworkErrorCode;
import io.seata.common.exception.FrameworkException;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.thread.PositiveAtomicCounter;
import io.seata.core.protocol.HeartbeatMessage;
import io.seata.core.protocol.MergeMessage;
import io.seata.core.protocol.MessageFuture;
import io.seata.core.protocol.MessageType;
import io.seata.core.protocol.MessageTypeAware;
import io.seata.core.protocol.ProtocolConstants;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.rpc.Disposable;
import io.seata.core.rpc.processor.Pair;
import io.seata.core.rpc.processor.RemotingProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The type Abstract rpc remoting.
 *
 * @author slievrly
 * @author zhangchenghui.dev@gmail.com
 */
public abstract class AbstractRpcRemoting implements Disposable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRpcRemoting.class);
    /**
     * The Timer executor.
     */
    protected final ScheduledExecutorService timerExecutor = new ScheduledThreadPoolExecutor(1,
        new NamedThreadFactory("timeoutChecker", 1, true));
    /**
     * The Message executor.
     */
    protected final ThreadPoolExecutor messageExecutor;

    /**
     * Id generator of this remoting
     */
    protected final PositiveAtomicCounter idGenerator = new PositiveAtomicCounter();

    /**
     * The Futures.
     * TODO: 用来存放 请求ID和响应Future，便于根据请求ID来获取异步对应的响应结果
     */
    protected final ConcurrentHashMap<Integer, MessageFuture> futures = new ConcurrentHashMap<>();
    /**
     * The Basket map.
     *  address ---> blockingQueue<RpcMessage>
     */
    protected final ConcurrentHashMap<String, BlockingQueue<RpcMessage>> basketMap = new ConcurrentHashMap<>();

    private static final long NOT_WRITEABLE_CHECK_MILLS = 10L;
    /**
     * The Merge lock.
     */
    protected final Object mergeLock = new Object();
    /**
     * The Now mills.
     */
    protected volatile long nowMills = 0;
    private static final int TIMEOUT_CHECK_INTERNAL = 3000;
    protected final Object lock = new Object();
    /**
     * The Is sending.
     */
    protected volatile boolean isSending = false;
    private String group = "DEFAULT";
    /**
     * The Merge msg map.
     */
    protected final Map<Integer, MergeMessage> mergeMsgMap = new ConcurrentHashMap<>();

    /**
     * This container holds all processors.
     * processor type {@link MessageType}
     */
    protected final HashMap<Integer/*MessageType*/, Pair<RemotingProcessor, ExecutorService>> processorTable = new HashMap<>(8);

    /**
     * Instantiates a new Abstract rpc remoting.
     *
     * @param messageExecutor the message executor
     */
    public AbstractRpcRemoting(ThreadPoolExecutor messageExecutor) {
        this.messageExecutor = messageExecutor;
    }

    /**
     * Gets next message id.
     *
     * @return the next message id
     */
    public int getNextMessageId() {
        return idGenerator.incrementAndGet();
    }

    public Map<Integer, MergeMessage> getMergeMsgMap() {
        return mergeMsgMap;
    }

    public ConcurrentHashMap<Integer, MessageFuture> getFutures() {
        return futures;
    }

    /**
     * TODO: 这个类初始化，所要做的事就是检查请求是否有超时的，如果超时则移除，然后把异常信息返回给它
     * Init.
     */
    public void init() {
        // TODO: futures 超时检查器，每隔3秒钟进行一个检查，如果有超时的进行一个移除，然后将结果设置为错误原因
        timerExecutor.scheduleAtFixedRate(() -> {
            // TODO: 遍历所有的futures, 如果超时了，从futures中移除
            for (Map.Entry<Integer, MessageFuture> entry : futures.entrySet()) {
                if (entry.getValue().isTimeout()) {
                    futures.remove(entry.getKey());
                    // 将对应的结果设置为null
                    entry.getValue().setResultMessage(null);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("timeout clear future: {}", entry.getValue().getRequestMessage().getBody());
                    }
                }
            }

            nowMills = System.currentTimeMillis();
        }, TIMEOUT_CHECK_INTERNAL, TIMEOUT_CHECK_INTERNAL, TimeUnit.MILLISECONDS);
    }

    /**
     * Destroy.
     */
    @Override
    public void destroy() {
        timerExecutor.shutdown();
        messageExecutor.shutdown();
    }

    /**
     * Send async request with response object.
     *
     * @param channel the channel
     * @param msg     the msg
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    protected Object sendAsyncRequestWithResponse(Channel channel, Object msg) throws TimeoutException {
        return sendAsyncRequestWithResponse(null, channel, msg, NettyClientConfig.getRpcRequestTimeout());
    }

    /**
     * Send async request with response object.
     *
     * @param address the address
     * @param channel the channel
     * @param msg     the msg
     * @param timeout the timeout
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    protected Object sendAsyncRequestWithResponse(String address, Channel channel, Object msg, long timeout) throws
        TimeoutException {
        // TODO: timeout 小于等于0，肯定要报错了
        if (timeout <= 0) {
            throw new FrameworkException("timeout should more than 0ms");
        }
        // TODO: 核心方法
        return sendAsyncRequest(address, channel, msg, timeout);
    }

    /**
     * Send async request without response object.
     *
     * @param channel the channel
     * @param msg     the msg
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    protected Object sendAsyncRequestWithoutResponse(Channel channel, Object msg) throws
        TimeoutException {
        return sendAsyncRequest(null, channel, msg, 0);
    }

    /**
     * 发送异步请求，核心方法，重中之重
     * @param address
     * @param channel
     * @param msg
     * @param timeout
     * @return
     * @throws TimeoutException
     */
    private Object sendAsyncRequest(String address, Channel channel, Object msg, long timeout)
        throws TimeoutException {
        if (channel == null) {
            LOGGER.warn("sendAsyncRequestWithResponse nothing, caused by null channel.");
            return null;
        }
        // TODO: 构建一个RpcMessage
        final RpcMessage rpcMessage = new RpcMessage();
        rpcMessage.setId(getNextMessageId());
        rpcMessage.setMessageType(ProtocolConstants.MSGTYPE_RESQUEST_ONEWAY);
        rpcMessage.setCodec(ProtocolConstants.CONFIGURED_CODEC);
        rpcMessage.setCompressor(ProtocolConstants.CONFIGURED_COMPRESSOR);
        rpcMessage.setBody(msg);

        /**
         * TODO: 构建一个响应messageFuture
         */
        final MessageFuture messageFuture = new MessageFuture();
        // 把request message 放进去
        messageFuture.setRequestMessage(rpcMessage);
        // 设置超时时间
        messageFuture.setTimeout(timeout);
        // 把rpcMessageId , 响应future放到futuresMap中
        futures.put(rpcMessage.getId(), messageFuture);

        /**
         * TODO: 如果地址不为空，并且开启了客户端批量发送
         */
        if (address != null) {
            /*
            The batch send.
            Object From big to small: RpcMessage -> MergedWarpMessage -> AbstractMessage
            @see AbstractRpcRemotingClient.MergedSendRunnable
                TODO: NettyClientConfig.isEnableClientBatchSendRequest()默认为true
            */
            if (NettyClientConfig.isEnableClientBatchSendRequest()) {
                // TODO: 把basketMap赋值给局部变量map, 然后从map中根据address取出blockQueue
                ConcurrentHashMap<String, BlockingQueue<RpcMessage>> map = basketMap;
                BlockingQueue<RpcMessage> basket = map.get(address);
                // TODO: 如果为空，存一个新的进去，然后再拿出来
                if (basket == null) {
                    map.putIfAbsent(address, new LinkedBlockingQueue<>());
                    basket = map.get(address);
                }
                // 然后把消息添加进basket中去
                basket.offer(rpcMessage);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("offer message: {}", rpcMessage.getBody());
                }
                // 如果未发送，则唤醒等待线程进行发送
                if (!isSending) {
                    // TODO: 加锁
                    synchronized (mergeLock) {
                        mergeLock.notifyAll();
                    }
                }
            } else {
                // the single send.
                sendSingleRequest(channel, msg, rpcMessage);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("send this msg[{}] by single send.", msg);
                }
            }
        } else {
            // TODO: 发送单个请求
            sendSingleRequest(channel, msg, rpcMessage);
        }
        /* ---------------- 开始等待异步响应 -------------- */
        // TODO: 如果设置等待时间了，等待超时时间，获取异步返回的结果
        if (timeout > 0) {
            try {
                return messageFuture.get(timeout, TimeUnit.MILLISECONDS);
            } catch (Exception exx) {
                LOGGER.error("wait response error:{},ip:{},request:{}", exx.getMessage(), address, msg);
                if (exx instanceof TimeoutException) {
                    throw (TimeoutException) exx;
                } else {
                    throw new RuntimeException(exx);
                }
            }
        } else {
            return null;
        }
    }

    private void sendSingleRequest(Channel channel, Object msg, RpcMessage rpcMessage) {
        ChannelFuture future;
        // TODO: 检查channel是否可写
        channelWritableCheck(channel, msg);
        // 可写 ，使用channel将message刷出去
        future = channel.writeAndFlush(rpcMessage);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                // TODO: 如果没有写成功
                if (!future.isSuccess()) {
                    // 把请求id从futures中移除，并且future不为空的话，把异常结果设置到result中
                    MessageFuture messageFuture = futures.remove(rpcMessage.getId());
                    if (messageFuture != null) {
                        messageFuture.setResultMessage(future.cause());
                    }
                    // TODO: 最后销毁这个channel
                    destroyChannel(future.channel());
                }
            }
        });
    }

    /**
     * Default Send request.
     *
     * @param channel the channel
     * @param msg     the msg
     */
    protected void defaultSendRequest(Channel channel, Object msg) {
        RpcMessage rpcMessage = new RpcMessage();
        rpcMessage.setMessageType(msg instanceof HeartbeatMessage ?
            ProtocolConstants.MSGTYPE_HEARTBEAT_REQUEST
            : ProtocolConstants.MSGTYPE_RESQUEST);
        rpcMessage.setCodec(ProtocolConstants.CONFIGURED_CODEC);
        rpcMessage.setCompressor(ProtocolConstants.CONFIGURED_COMPRESSOR);
        rpcMessage.setBody(msg);
        rpcMessage.setId(getNextMessageId());
        if (msg instanceof MergeMessage) {
            mergeMsgMap.put(rpcMessage.getId(), (MergeMessage) msg);
        }
        channelWritableCheck(channel, msg);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("write message:" + rpcMessage.getBody() + ", channel:" + channel + ",active?"
                + channel.isActive() + ",writable?" + channel.isWritable() + ",isopen?" + channel.isOpen());
        }
        channel.writeAndFlush(rpcMessage);
    }

    /**
     * Default Send response.
     * TODO: 默认发送响应
     * @param request the msg id
     * @param channel the channel
     * @param msg     the msg
     */
    protected void defaultSendResponse(RpcMessage request, Channel channel, Object msg) {
        RpcMessage rpcMessage = new RpcMessage();
        // TODO: 如果当前msg是心跳请求message，则响应类型为心跳响应，否则为msg_response
        rpcMessage.setMessageType(msg instanceof HeartbeatMessage ?
            ProtocolConstants.MSGTYPE_HEARTBEAT_RESPONSE :
            ProtocolConstants.MSGTYPE_RESPONSE);
        // same with request
        // TODO: 编解码和request请求一样
        rpcMessage.setCodec(request.getCodec());
        // TODO: 压缩码和请求的一样
        rpcMessage.setCompressor(request.getCompressor());
        // TODO: 设置响应体
        rpcMessage.setBody(msg);
        // 设置响应的消息id，和request.getId是一样的
        rpcMessage.setId(request.getId());
        // TODO: 检查channel是否可写
        channelWritableCheck(channel, msg);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("send response:" + rpcMessage.getBody() + ",channel:" + channel);
        }
        // 发送响应
        channel.writeAndFlush(rpcMessage);
    }


    /**
     * TODO: 检查channel是否可写
     * @param channel
     * @param msg
     */
    private void channelWritableCheck(Channel channel, Object msg) {
        // 重试次数
        int tryTimes = 0;
        // TODO: 这里加锁，个人感觉是，可能会有多个线程同时检查一个channel是否可写， 如果一个线程检查可写了，然后进行写入，此时，另外的线程是需要等待的
        synchronized (lock) {
            while (!channel.isWritable()) {
                try {
                    tryTimes++;
                    // 判断重试次数 是否大于 配置的重试次数，如果大于了
                    if (tryTimes > NettyClientConfig.getMaxNotWriteableRetry()) {
                        // 销毁channel
                        destroyChannel(channel);
                        // 抛个异常，当前channel不可写
                        throw new FrameworkException("msg:" + ((msg == null) ? "null" : msg.toString()),
                            FrameworkErrorCode.ChannelIsNotWritable);
                    }
                    // TODO 等待几秒钟，继续检查, 这里注意，采用的是lock.wait(); 表示其他线程是可以获得锁，然后进行判断channel读写状态的
                    lock.wait(NOT_WRITEABLE_CHECK_MILLS);
                } catch (InterruptedException exx) {
                    LOGGER.error(exx.getMessage());
                }
            }
        }
    }

    /**
     * Gets group.
     *
     * @return the group
     */
    public String getGroup() {
        return group;
    }

    /**
     * Sets group.
     *
     * @param group the group
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * Destroy channel.
     *
     * @param channel the channel
     */
    public void destroyChannel(Channel channel) {
        destroyChannel(getAddressFromChannel(channel), channel);
    }

    /**
     * Destroy channel.
     *
     * @param serverAddress the server address
     * @param channel       the channel
     */
    public abstract void destroyChannel(String serverAddress, Channel channel);

    /**
     * Gets address from context.
     *
     * @param ctx the ctx
     * @return the address from context
     */
    protected String getAddressFromContext(ChannelHandlerContext ctx) {
        return getAddressFromChannel(ctx.channel());
    }

    /**
     * Gets address from channel.
     *
     * @param channel the channel
     * @return the address from channel
     */
    protected String getAddressFromChannel(Channel channel) {
        SocketAddress socketAddress = channel.remoteAddress();
        String address = socketAddress.toString();
        if (socketAddress.toString().indexOf(NettyClientConfig.getSocketAddressStartChar()) == 0) {
            address = socketAddress.toString().substring(NettyClientConfig.getSocketAddressStartChar().length());
        }
        return address;
    }

    /**
     * For testing. When the thread pool is full, you can change this variable and share the stack
     */
    boolean allowDumpStack = false;


    /**
     * Rpc message processing.
     * TODO: 处理rpc Message
     *
     * @param ctx        Channel handler context.
     * @param rpcMessage rpc message.
     * @throws Exception throws exception process message error.
     * @since 1.3.0
     */
    protected void processMessage(ChannelHandlerContext ctx, RpcMessage rpcMessage) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("%s msgId:%s, body:%s", this, rpcMessage.getId(), rpcMessage.getBody()));
        }
        // TODO: 拿到message请求体
        Object body = rpcMessage.getBody();
        if (body instanceof MessageTypeAware) {
            // TODO: 如果是MessageTypeAware类型的，则进行一个强转
            MessageTypeAware messageTypeAware = (MessageTypeAware) body;
            // 从processorTable中根据typeCode拿到 处理器和执行它的线程池
            final Pair<RemotingProcessor, ExecutorService> pair = this.processorTable.get((int) messageTypeAware.getTypeCode());
            if (pair != null) {
                // TODO: 线程池不为空，利用线程池去执行
                if (pair.getSecond() != null) {
                    try {
                        pair.getSecond().execute(() -> {
                            try {
                                // TODO: 执行RemotingProcessor的process方法
                                pair.getFirst().process(ctx, rpcMessage);
                            } catch (Throwable th) {
                                LOGGER.error(FrameworkErrorCode.NetDispatch.getErrCode(), th.getMessage(), th);
                            }
                        });
                        // TODO: 这里catch住线程池拒绝异常，表示队列已满，已到最大线程数
                    } catch (RejectedExecutionException e) {
                        // TODO: 打印错误日志
                        LOGGER.error(FrameworkErrorCode.ThreadPoolFull.getErrCode(),
                            "thread pool is full, current max pool size is " + messageExecutor.getActiveCount());
                        // TODO: 如果开启了打印堆栈信息，则打印
                        if (allowDumpStack) {
                            String name = ManagementFactory.getRuntimeMXBean().getName();
                            String pid = name.split("@")[0];
                            int idx = new Random().nextInt(100);
                            try {
                                Runtime.getRuntime().exec("jstack " + pid + " >d:/" + idx + ".log");
                            } catch (IOException exx) {
                                LOGGER.error(exx.getMessage());
                            }
                            allowDumpStack = false;
                        }
                    }
                } else {
                    // 没有线程池，则直接进行调用 处理
                    try {
                        pair.getFirst().process(ctx, rpcMessage);
                    } catch (Throwable th) {
                        LOGGER.error(FrameworkErrorCode.NetDispatch.getErrCode(), th.getMessage(), th);
                    }
                }
            } else {
                LOGGER.error("This message type [{}] has no processor.", messageTypeAware.getTypeCode());
            }
        } else {
            LOGGER.error("This rpcMessage body[{}] is not MessageTypeAware type.", body);
        }
    }

    /**
     * The type AbstractHandler.
     * TODO: 一个很重要的内部类，extends 了 ChannelDuplexHandler
     */
    @Deprecated
    abstract class AbstractHandler extends ChannelDuplexHandler {

        /**
         * Dispatch.
         *
         * @param request the request
         * @param ctx     the ctx
         */
        public abstract void dispatch(RpcMessage request, ChannelHandlerContext ctx);

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) {
            synchronized (lock) {
                if (ctx.channel().isWritable()) {
                    lock.notifyAll();
                }
            }

            ctx.fireChannelWritabilityChanged();
        }

        /**
         * TODO: 负责处理读事件
         * @param ctx
         * @param msg
         * @throws Exception
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
            // TODO: 如果msg为 RPCMessage 才进行处理
            if (msg instanceof RpcMessage) {
                final RpcMessage rpcMessage = (RpcMessage) msg;
                // TODO: 判断消息类型为请求类型
                if (rpcMessage.getMessageType() == ProtocolConstants.MSGTYPE_RESQUEST
                    || rpcMessage.getMessageType() == ProtocolConstants.MSGTYPE_RESQUEST_ONEWAY) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String.format("%s msgId:%s, body:%s", this, rpcMessage.getId(), rpcMessage.getBody()));
                    }
                    try {
                        // TODO: 使用messageExecutor 去处理 请求消息
                        messageExecutor.execute(() -> {
                            try {
                                // TODO: 分发消息 去处理，由子类去实现
                                dispatch(rpcMessage, ctx);
                            } catch (Throwable th) {
                                LOGGER.error(FrameworkErrorCode.NetDispatch.getErrCode(), th.getMessage(), th);
                            }
                        });
                    } catch (RejectedExecutionException e) {
                        // TODO: 如果发生拒绝策略异常，和上面的处理一样
                        LOGGER.error(FrameworkErrorCode.ThreadPoolFull.getErrCode(),
                            "thread pool is full, current max pool size is " + messageExecutor.getActiveCount());
                        if (allowDumpStack) {
                            String name = ManagementFactory.getRuntimeMXBean().getName();
                            String pid = name.split("@")[0];
                            int idx = new Random().nextInt(100);
                            try {
                                Runtime.getRuntime().exec("jstack " + pid + " >d:/" + idx + ".log");
                            } catch (IOException exx) {
                                LOGGER.error(exx.getMessage());
                            }
                            allowDumpStack = false;
                        }
                    }
                } else {
                    // TODO: 不是请求类型，则直接从futures拿出请求 响应
                    MessageFuture messageFuture = futures.remove(rpcMessage.getId());
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String
                            .format("%s msgId:%s, future :%s, body:%s", this, rpcMessage.getId(), messageFuture,
                                rpcMessage.getBody()));
                    }
                    // TODO: messageFuture不为空，设置响应结果
                    if (messageFuture != null) {
                        messageFuture.setResultMessage(rpcMessage.getBody());
                    } else {
                        try {
                            // TODO: 如果messageFuture为空，则用线程池 去执行
                            messageExecutor.execute(() -> {
                                try {
                                    dispatch(rpcMessage, ctx);
                                } catch (Throwable th) {
                                    LOGGER.error(FrameworkErrorCode.NetDispatch.getErrCode(), th.getMessage(), th);
                                }
                            });
                        } catch (RejectedExecutionException e) {
                            LOGGER.error(FrameworkErrorCode.ThreadPoolFull.getErrCode(),
                                "thread pool is full, current max pool size is " + messageExecutor.getActiveCount());
                        }
                    }
                }
            }
        }

        /**
         * TODO: 如果发生异常，打印日志，然后销毁channel
         * @param ctx
         * @param cause
         * @throws Exception
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOGGER.error(FrameworkErrorCode.ExceptionCaught.getErrCode(),
                ctx.channel() + " connect exception. " + cause.getMessage(),
                cause);
            try {
                destroyChannel(ctx.channel());
            } catch (Exception e) {
                LOGGER.error("failed to close channel {}: {}", ctx.channel(), e.getMessage(), e);
            }
        }

        /**
         * 关闭channel
         * @param ctx
         * @param future
         * @throws Exception
         */
        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(ctx + " will closed");
            }
            super.close(ctx, future);
        }

    }
}
