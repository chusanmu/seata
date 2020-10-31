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
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.seata.common.exception.FrameworkErrorCode;
import io.seata.common.exception.FrameworkException;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.thread.PositiveAtomicCounter;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The abstract netty remoting.
 *
 * @author slievrly
 * @author zhangchenghui.dev@gmail.com
 */
public abstract class AbstractNettyRemoting implements Disposable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNettyRemoting.class);
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
     * TODO: id 生成器，可以恒定生成正数
     * Id generator of this remoting
     */
    protected final PositiveAtomicCounter idGenerator = new PositiveAtomicCounter();

    /**
     * TODO: 通过messageFuture 阻塞来获取结果
     * 
     * Obtain the return result through MessageFuture blocking.
     *
     * @see AbstractNettyRemoting#sendSync
     */
    protected final ConcurrentHashMap<Integer, MessageFuture> futures = new ConcurrentHashMap<>();

    private static final long NOT_WRITEABLE_CHECK_MILLS = 10L;

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
     * This container holds all processors.
     * processor type {@link MessageType}
     */
    protected final HashMap<Integer/*MessageType*/, Pair<RemotingProcessor, ExecutorService>> processorTable = new HashMap<>(32);

    public void init() {
        // TODO: 定时器，用于定时future中超时的请求
        timerExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for (Map.Entry<Integer, MessageFuture> entry : futures.entrySet()) {
                    // TODO: 如果这个future已经超时了
                    if (entry.getValue().isTimeout()) {
                        // TODO: 就把它从futures中移除
                        futures.remove(entry.getKey());
                        // TODO: 同时将future的 resultMessage设置为null
                        entry.getValue().setResultMessage(null);
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("timeout clear future: {}", entry.getValue().getRequestMessage().getBody());
                        }
                    }
                }

                nowMills = System.currentTimeMillis();
            }
        }, TIMEOUT_CHECK_INTERNAL, TIMEOUT_CHECK_INTERNAL, TimeUnit.MILLISECONDS);
    }

    public AbstractNettyRemoting(ThreadPoolExecutor messageExecutor) {
        this.messageExecutor = messageExecutor;
    }

    /**
     * 生成消息ID
     *
     * @return
     */
    public int getNextMessageId() {
        return idGenerator.incrementAndGet();
    }

    public ConcurrentHashMap<Integer, MessageFuture> getFutures() {
        return futures;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public void destroyChannel(Channel channel) {
        destroyChannel(getAddressFromChannel(channel), channel);
    }

    /**
     * TODO: 把两个线程池 shutdown();
     */
    @Override
    public void destroy() {
        timerExecutor.shutdown();
        messageExecutor.shutdown();
    }

    /**
     * 发起 rpc 同步请求
     * rpc sync request
     * Obtain the return result through MessageFuture blocking.
     *
     * @param channel       netty channel
     * @param rpcMessage    rpc message
     * @param timeoutMillis rpc communication timeout
     * @return response message
     * @throws TimeoutException
     */
    protected Object sendSync(Channel channel, RpcMessage rpcMessage, long timeoutMillis) throws TimeoutException {
        // TODO: 校验参数的合法性
        if (timeoutMillis <= 0) {
            throw new FrameworkException("timeout should more than 0ms");
        }
        if (channel == null) {
            LOGGER.warn("sendSync nothing, caused by null channel.");
            return null;
        }

        // TODO: 构建响应 messageFuture
        MessageFuture messageFuture = new MessageFuture();
        messageFuture.setRequestMessage(rpcMessage);
        messageFuture.setTimeout(timeoutMillis);
        futures.put(rpcMessage.getId(), messageFuture);

        // TODO: 校验当前channel是否可写
        channelWritableCheck(channel, rpcMessage.getBody());

        channel.writeAndFlush(rpcMessage).addListener((ChannelFutureListener) future -> {
            // TODO: 如果future没有成功
            if (!future.isSuccess()) {
                // TODO: 把future 取出来 把结果设置成 future.cause(), 然后销毁channel
                MessageFuture messageFuture1 = futures.remove(rpcMessage.getId());
                if (messageFuture1 != null) {
                    messageFuture1.setResultMessage(future.cause());
                }
                // TODO: 销毁channel
                destroyChannel(future.channel());
            }
        });

        try {
            // TODO: 从future中取出结果 返回回去
            return messageFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Exception exx) {
            LOGGER.error("wait response error:{},ip:{},request:{}", exx.getMessage(), channel.remoteAddress(),
                rpcMessage.getBody());
            if (exx instanceof TimeoutException) {
                throw (TimeoutException) exx;
            } else {
                throw new RuntimeException(exx);
            }
        }
    }

    /**
     * rpc async request.
     * 发起一个异步请求
     *
     * @param channel    netty channel
     * @param rpcMessage rpc message
     */
    protected void sendAsync(Channel channel, RpcMessage rpcMessage) {
        // TODO: 检测当前channel是否可写
        channelWritableCheck(channel, rpcMessage.getBody());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("write message:" + rpcMessage.getBody() + ", channel:" + channel + ",active?"
                + channel.isActive() + ",writable?" + channel.isWritable() + ",isopen?" + channel.isOpen());
        }
        // TODO: 写出消息，如果失败了，就销毁这个channel
        channel.writeAndFlush(rpcMessage).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                destroyChannel(future.channel());
            }
        });
    }

    /**
     * 构建 request message
     *
     * @param msg
     * @param messageType
     * @return
     */
    protected RpcMessage buildRequestMessage(Object msg, byte messageType) {
        RpcMessage rpcMessage = new RpcMessage();
        rpcMessage.setId(getNextMessageId());
        rpcMessage.setMessageType(messageType);
        rpcMessage.setCodec(ProtocolConstants.CONFIGURED_CODEC);
        rpcMessage.setCompressor(ProtocolConstants.CONFIGURED_COMPRESSOR);
        rpcMessage.setBody(msg);
        return rpcMessage;
    }

    /**
     * 构建 响应消息
     *
     * @param rpcMessage
     * @param msg
     * @param messageType
     * @return
     */
    protected RpcMessage buildResponseMessage(RpcMessage rpcMessage, Object msg, byte messageType) {
        RpcMessage rpcMsg = new RpcMessage();
        rpcMsg.setMessageType(messageType);
        rpcMsg.setCodec(rpcMessage.getCodec()); // same with request
        rpcMsg.setCompressor(rpcMessage.getCompressor());
        rpcMsg.setBody(msg);
        rpcMsg.setId(rpcMessage.getId());
        return rpcMsg;
    }

    /**
     * For testing. When the thread pool is full, you can change this variable and share the stack
     */
    boolean allowDumpStack = false;

    /**
     * TODO: 核心方法
     * Rpc message processing.
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
        // TODO: 把消息体拿过来， 获取messageType
        Object body = rpcMessage.getBody();
        if (body instanceof MessageTypeAware) {
            // TODO: 转成messageTypeAware
            MessageTypeAware messageTypeAware = (MessageTypeAware) body;
            // TODO: 从processorTable中获取可以处理此消息的 pair(消息处理器，执行线程池)
            final Pair<RemotingProcessor, ExecutorService> pair = this.processorTable.get((int) messageTypeAware.getTypeCode());
            if (pair != null) {
                // TODO: 当前的消息 存在pair, 可以被处理，存在执行它的线程池
                if (pair.getSecond() != null) {
                    try {
                        // TODO: 利用此处理器的线程池去处理
                        pair.getSecond().execute(() -> {
                            try {
                                pair.getFirst().process(ctx, rpcMessage);
                            } catch (Throwable th) {
                                LOGGER.error(FrameworkErrorCode.NetDispatch.getErrCode(), th.getMessage(), th);
                            }
                        });
                        // TODO: 当线程池满的时候，可以进行dump当前线程池 来排查问题
                    } catch (RejectedExecutionException e) {
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
                    // TODO: 没有线程池，则直接利用当前线程去执行了
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
        // TODO: 把开头的 / 去掉
        if (socketAddress.toString().indexOf(NettyClientConfig.getSocketAddressStartChar()) == 0) {
            address = socketAddress.toString().substring(NettyClientConfig.getSocketAddressStartChar().length());
        }
        return address;
    }

    /**
     * TODO: 检查当前channel是否可写
     *
     * @param channel
     * @param msg
     */
    private void channelWritableCheck(Channel channel, Object msg) {
        int tryTimes = 0;
        synchronized (lock) {
            while (!channel.isWritable()) {
                try {
                    tryTimes++;
                    if (tryTimes > NettyClientConfig.getMaxNotWriteableRetry()) {
                        destroyChannel(channel);
                        throw new FrameworkException("msg:" + ((msg == null) ? "null" : msg.toString()),
                            FrameworkErrorCode.ChannelIsNotWritable);
                    }
                    lock.wait(NOT_WRITEABLE_CHECK_MILLS);
                } catch (InterruptedException exx) {
                    LOGGER.error(exx.getMessage());
                }
            }
        }
    }

    /**
     * Destroy channel.
     *
     * @param serverAddress the server address
     * @param channel       the channel
     */
    public abstract void destroyChannel(String serverAddress, Channel channel);

}
