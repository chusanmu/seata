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
package io.seata.core.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleBiFunction;

import io.netty.channel.ChannelHandlerContext;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.NetUtil;
import io.seata.core.protocol.AbstractMessage;
import io.seata.core.protocol.AbstractResultMessage;
import io.seata.core.protocol.HeartbeatMessage;
import io.seata.core.protocol.MergeResultMessage;
import io.seata.core.protocol.MergedWarpMessage;
import io.seata.core.protocol.RegisterRMRequest;
import io.seata.core.protocol.RegisterRMResponse;
import io.seata.core.protocol.RegisterTMRequest;
import io.seata.core.protocol.RegisterTMResponse;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.protocol.Version;
import io.seata.core.rpc.netty.RegisterCheckAuthHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Default server message listener.
 * 默认的Message Listener 消息监听处理器
 *
 * @author slievrly
 */
public class DefaultServerMessageListenerImpl implements ServerMessageListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultServerMessageListenerImpl.class);
    private static BlockingQueue<String> logQueue = new LinkedBlockingQueue<>();
    private ServerMessageSender serverMessageSender;
    /**
     * TODO: rpc消息处理器，当收到响应时，或者收到请求时，进行处理
     */
    private final TransactionMessageHandler transactionMessageHandler;
    /**
     * 最大的日志打印线程个数
     */
    private static final int MAX_LOG_SEND_THREAD = 1;
    /**
     * 最大一次性输出多少条日志
     */
    private static final int MAX_LOG_TAKE_SIZE = 1024;
    /**
     * 核心 线程个数
     */
    private static final long KEEP_ALIVE_TIME = 0L;


    private static final String THREAD_PREFIX = "batchLoggerPrint";
    private static final long BUSY_SLEEP_MILLS = 5L;

    /**
     * Instantiates a new Default server message listener.
     *
     * @param transactionMessageHandler the transaction message handler
     */
    public DefaultServerMessageListenerImpl(TransactionMessageHandler transactionMessageHandler) {
        this.transactionMessageHandler = transactionMessageHandler;
    }

    @Override
    public void onTrxMessage(RpcMessage request, ChannelHandlerContext ctx) {
        Object message = request.getBody();
        // TODO: 从当前channel 取出请求上下文
        RpcContext rpcContext = ChannelManager.getContextFromIdentified(ctx.channel());
        // TODO: 打印一个debug信息，如果没有开启debug，则把它堆到 logQueue中，然后进行 用额外的线程进行隔时间打印就ok了呀
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("server received:{},clientIp:{},vgroup:{}", message,
                    NetUtil.toIpAddress(ctx.channel().remoteAddress()), rpcContext.getTransactionServiceGroup());
        } else {
            try {
                // TODO: 把日志放到logQueue中啊
                logQueue.put(message + ",clientIp:" + NetUtil.toIpAddress(ctx.channel().remoteAddress()) + ",vgroup:"
                        + rpcContext.getTransactionServiceGroup());
            } catch (InterruptedException e) {
                LOGGER.error("put message to logQueue error: {}", e.getMessage(), e);
            }
        }
        // TODO: 你不是AbstractMessage类型的，你就直接返回就可以了啊
        if (!(message instanceof AbstractMessage)) {
            return;
        }
        // TODO: 如果是一个Merge 包装的消息类型
        if (message instanceof MergedWarpMessage) {
            // TODO: 构建响应啊，resultMessage的个数 是 要和聚合消息中的消息个数是相等的
            AbstractResultMessage[] results = new AbstractResultMessage[((MergedWarpMessage) message).msgs.size()];
            for (int i = 0; i < results.length; i++) {
                // TODO: 把对应的每个subMessage取出来，然后用message处理器去处理
                final AbstractMessage subMessage = ((MergedWarpMessage) message).msgs.get(i);
                // TODO: 处理完成 之后会返回一个响应， 具体的可能会交给一个协调器去处理
                results[i] = transactionMessageHandler.onRequest(subMessage, rpcContext);
            }
            // TODO: 把每个消息的响应放进去
            MergeResultMessage resultMessage = new MergeResultMessage();
            resultMessage.setMsgs(results);
            // TODO: 最后发出去
            getServerMessageSender().sendResponse(request, ctx.channel(), resultMessage);
        } else if (message instanceof AbstractResultMessage) {
            // TODO: 如果消息类型是一个响应消息，则去处理响应呗
            transactionMessageHandler.onResponse((AbstractResultMessage) message, rpcContext);
        } else {
            // the single send request message
            // TODO: 否则就是单独的一个request请求
            final AbstractMessage msg = (AbstractMessage) message;
            // 处理器请求，发送响应
            AbstractResultMessage result = transactionMessageHandler.onRequest(msg, rpcContext);
            getServerMessageSender().sendResponse(request, ctx.channel(), result);
        }
    }

    /**
     * TODO: 当RM过来注册的时候，进行处理
     *
     * @param request          the msg id
     * @param ctx              the ctx
     * @param checkAuthHandler the check auth handler
     */
    @Override
    public void onRegRmMessage(RpcMessage request, ChannelHandlerContext ctx, RegisterCheckAuthHandler checkAuthHandler) {
        // TODO: 得到请求
        RegisterRMRequest message = (RegisterRMRequest) request.getBody();
        // TODO: 拿到IP和端口号
        String ipAndPort = NetUtil.toStringAddress(ctx.channel().remoteAddress());
        boolean isSuccess = false;
        try {
            if (null == checkAuthHandler || checkAuthHandler.regResourceManagerCheckAuth(message)) {
                // TODO: 向channel 管理器中注册一个RM channel
                ChannelManager.registerRMChannel(message, ctx.channel());
                Version.putChannelVersion(ctx.channel(), message.getVersion());
                // TODO: 标识成功
                isSuccess = true;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("checkAuth for client:{},vgroup:{},applicationId:{}",
                            ipAndPort, message.getTransactionServiceGroup(), message.getApplicationId());
                }
            }
        } catch (Exception exx) {
            // TODO: 发送异常，标识isSuccess为false
            isSuccess = false;
            LOGGER.error(exx.getMessage());
        }
        // TODO: 返回响应，通过messageSender 发送 响应，将是否成功放进去
        getServerMessageSender().sendResponse(request, ctx.channel(), new RegisterRMResponse(isSuccess));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("RM register success,message:{},channel:{}", message, ctx.channel());
        }
    }

    /**
     * TODO 处理注册TM的消息
     *
     * @param request          the msg id
     * @param ctx              the ctx
     * @param checkAuthHandler the check auth handler
     */
    @Override
    public void onRegTmMessage(RpcMessage request, ChannelHandlerContext ctx, RegisterCheckAuthHandler checkAuthHandler) {
        // TODO: 拿到message
        RegisterTMRequest message = (RegisterTMRequest) request.getBody();
        // TODO: 拿到IP和端口号
        String ipAndPort = NetUtil.toStringAddress(ctx.channel().remoteAddress());
        Version.putChannelVersion(ctx.channel(), message.getVersion());
        boolean isSuccess = false;
        try {
            if (null == checkAuthHandler || checkAuthHandler.regTransactionManagerCheckAuth(message)) {
                // TODO: 向channelManager中 注册一个TM channel
                ChannelManager.registerTMChannel(message, ctx.channel());
                Version.putChannelVersion(ctx.channel(), message.getVersion());
                isSuccess = true;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("checkAuth for client:{},vgroup:{},applicationId:{}",
                            ipAndPort, message.getTransactionServiceGroup(), message.getApplicationId());
                }
            }
        } catch (Exception exx) {
            isSuccess = false;
            LOGGER.error(exx.getMessage());
        }
        // TODO: 返回响应
        getServerMessageSender().sendResponse(request, ctx.channel(), new RegisterTMResponse(isSuccess));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("TM register success,message:{},channel:{}", message, ctx.channel());
        }
    }

    @Override
    public void onCheckMessage(RpcMessage request, ChannelHandlerContext ctx) {
        try {
            // TODO: 如果是监测消息(心跳)，就直接给它响应个pong 就完活了
            getServerMessageSender().sendResponse(request, ctx.channel(), HeartbeatMessage.PONG);
        } catch (Throwable throwable) {
            LOGGER.error("send response error: {}", throwable.getMessage(), throwable);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received PING from {}", ctx.channel().remoteAddress());
        }
    }

    /**
     * Init.
     * TODO: 初始化方法
     */
    public void init() {
        /**
         * 这里初始化了一个 线程池，主要做的就是，取出需要打印的日志，进行一个批量打印，每隔5毫秒进行一次打印
         */
        ExecutorService mergeSendExecutorService = new ThreadPoolExecutor(MAX_LOG_SEND_THREAD, MAX_LOG_SEND_THREAD,
                KEEP_ALIVE_TIME, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory(THREAD_PREFIX, MAX_LOG_SEND_THREAD, true));
        // 去执行呗
        mergeSendExecutorService.submit(new BatchLogRunnable());
    }

    /**
     * Gets server message sender.
     *
     * @return the server message sender
     */
    public ServerMessageSender getServerMessageSender() {
        if (serverMessageSender == null) {
            throw new IllegalArgumentException("serverMessageSender must not be null");
        }
        return serverMessageSender;
    }

    /**
     * Sets server message sender.
     *
     * @param serverMessageSender the server message sender
     */
    public void setServerMessageSender(ServerMessageSender serverMessageSender) {
        this.serverMessageSender = serverMessageSender;
    }

    /**
     * The type Batch log runnable.
     */
    static class BatchLogRunnable implements Runnable {

        @Override
        public void run() {
            List<String> logList = new ArrayList<>();
            while (true) {
                try {
                    // TODO: 这行代码是不可以删除的 注意，之后还是会把logQueue中的日志 加到 logList中
                    logList.add(logQueue.take());
                    // TODO: drainTo方法是非阻塞的，上面加一句logQueue.take()可以阻塞住线程
                    logQueue.drainTo(logList, MAX_LOG_TAKE_SIZE);
                    // TODO: 如果开启了打印日志，则进行输出打印
                    if (LOGGER.isInfoEnabled()) {
                        for (String str : logList) {
                            LOGGER.info(str);
                        }
                    }
                    // TODO: 最后打印完了，进行一波清空
                    logList.clear();
                    // TODO: sleep一会儿
                    TimeUnit.MILLISECONDS.sleep(BUSY_SLEEP_MILLS);
                } catch (InterruptedException exx) {
                    LOGGER.error("batch log busy sleep error:{}", exx.getMessage(), exx);
                }

            }
        }
    }

}
