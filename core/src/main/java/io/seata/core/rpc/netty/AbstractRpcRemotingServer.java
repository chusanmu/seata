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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.seata.common.util.NetUtil;
import io.seata.core.protocol.HeartbeatMessage;
import io.seata.core.protocol.RegisterRMRequest;
import io.seata.core.protocol.RegisterTMRequest;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * The type Rpc remoting server.
 * TODO: 抽象的Rpc远程server, 继承了AbstractRpcRemoting, 然后实现了ServerMessageSender接口
 * 这里关于ServerMessageSender 的具体实现，实际上是交给了AbstractRpcRemoting 去做的
 * @author slievrly
 * @author xingfudeshi@gmail.com
 */
public abstract class AbstractRpcRemotingServer extends AbstractRpcRemoting implements ServerMessageSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRpcRemotingServer.class);


    /**
     * seata server的启动类
     */
    private final RpcServerBootstrap serverBootstrap;

    /**
     * The Server message listener.
     */
    private ServerMessageListener serverMessageListener;

    private TransactionMessageHandler transactionMessageHandler;

    private RegisterCheckAuthHandler checkAuthHandler;

    /**
     * Instantiates a new Rpc remoting server.
     *
     * @param messageExecutor   the message executor
     * @param nettyServerConfig the netty server config
     */
    public AbstractRpcRemotingServer(final ThreadPoolExecutor messageExecutor, NettyServerConfig nettyServerConfig) {
        // 接着向上传
        super(messageExecutor);
        // 在这里初始化rpcServerBootstrap();
        serverBootstrap = new RpcServerBootstrap(nettyServerConfig);
    }

    /**
     * Sets transactionMessageHandler.
     *
     * @param transactionMessageHandler the transactionMessageHandler
     */
    public void setHandler(TransactionMessageHandler transactionMessageHandler) {
        setHandler(transactionMessageHandler, null);
    }

    private void setHandler(TransactionMessageHandler transactionMessageHandler, RegisterCheckAuthHandler checkAuthHandler) {
        this.transactionMessageHandler = transactionMessageHandler;
        this.checkAuthHandler = checkAuthHandler;
    }

    public TransactionMessageHandler getTransactionMessageHandler() {
        return transactionMessageHandler;
    }

    public RegisterCheckAuthHandler getCheckAuthHandler() {
        return checkAuthHandler;
    }

    /**
     * Sets server message listener.
     *
     * @param serverMessageListener the server message listener
     */
    public void setServerMessageListener(ServerMessageListener serverMessageListener) {
        this.serverMessageListener = serverMessageListener;
    }

    /**
     * Gets server message listener.
     *
     * @return the server message listener
     */
    public ServerMessageListener getServerMessageListener() {
        return serverMessageListener;
    }

    /**
     * Sets channel handlers.
     *
     * @param handlers the handlers
     */
    public void setChannelHandlers(ChannelHandler... handlers) {
        serverBootstrap.setChannelHandlers(handlers);
    }

    /**
     * Sets listen port.
     *
     * @param listenPort the listen port
     */
    public void setListenPort(int listenPort) {
        serverBootstrap.setListenPort(listenPort);
    }

    /**
     * Gets listen port.
     *
     * @return the listen port
     */
    public int getListenPort() {
        return serverBootstrap.getListenPort();
    }

    @Override
    public void init() {
        // TODO: 去初始化父类，检查请求是否有超时的，如果超时，移除请求，然后把异常信息设置到请求对应的响应中
        super.init();
        // TODO: 开始启动seata-server服务，启动服务!
        serverBootstrap.start();
    }

    @Override
    public void destroy() {
        serverBootstrap.shutdown();
        super.destroy();
    }

    /**
     * Debug log.
     *
     * @param info the info
     */
    public void debugLog(String info) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(info);
        }
    }

    private void closeChannelHandlerContext(ChannelHandlerContext ctx) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("closeChannelHandlerContext channel:" + ctx.channel());
        }
        ctx.disconnect();
        ctx.close();
    }

    @Override
    public void destroyChannel(String serverAddress, Channel channel) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("will destroy channel:{},address:{}", channel, serverAddress);
        }
        channel.disconnect();
        channel.close();
    }

    /**
     * The type ServerHandler.
     */
    @ChannelHandler.Sharable
    class ServerHandler extends AbstractHandler {

        /**
         * Dispatch.
         *
         * @param request the request
         * @param ctx     the ctx
         */
        @Override
        public void dispatch(RpcMessage request, ChannelHandlerContext ctx) {
            Object msg = request.getBody();
            // TODO: 判断如果是注册RM的请求，则去进行处理呀
            if (msg instanceof RegisterRMRequest) {
                serverMessageListener.onRegRmMessage(request, ctx, checkAuthHandler);
            } else {
                // TODO: 否则判断 当前channel是否已经注册完成了
                if (ChannelManager.isRegistered(ctx.channel())) {
                    // 如果已经注册完成，则直接去处理request 就ok了
                    serverMessageListener.onTrxMessage(request, ctx);
                } else {
                    try {
                        closeChannelHandlerContext(ctx);
                    } catch (Exception exx) {
                        LOGGER.error(exx.getMessage());
                    }
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(String.format("close a unhandled connection! [%s]", ctx.channel().toString()));
                    }
                }
            }
        }

        /**
         * Channel read.
         *
         * @param ctx the ctx
         * @param msg the msg
         * @throws Exception the exception
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
            /**
             * TODO: 这里判断 注册TM，判断注册RM 判断心跳
             * 这里子类只进行了注册TM，以及心跳请求的判断，而将注册RM的请求放到了父类中去判断，之后交由dispatch去判断
             * 这里的思路 可能是RM可能会有多个，然后交由dispatch去多线程调度处理，而TM通常只有一个，所以放在了这里去判断，这部分代码，之后可能会进行重构
             */
            if (msg instanceof RpcMessage) {
                RpcMessage rpcMessage = (RpcMessage) msg;
                debugLog("read:" + rpcMessage.getBody());
                // TODO: 判断当前请求 为 注册TM的请求
                if (rpcMessage.getBody() instanceof RegisterTMRequest) {
                    serverMessageListener.onRegTmMessage(rpcMessage, ctx, checkAuthHandler);
                    return;
                }
                // TODO: 如果是 心跳请求，直接进行返回PONG
                if (rpcMessage.getBody() == HeartbeatMessage.PING) {
                    serverMessageListener.onCheckMessage(rpcMessage, ctx);
                    return;
                }
            }
            super.channelRead(ctx, msg);
        }

        /**
         * Channel inactive.
         *
         * @param ctx the ctx
         * @throws Exception the exception
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            debugLog("inactive:" + ctx);
            if (messageExecutor.isShutdown()) {
                return;
            }
            handleDisconnect(ctx);
            super.channelInactive(ctx);
        }

        private void handleDisconnect(ChannelHandlerContext ctx) {
            final String ipAndPort = NetUtil.toStringAddress(ctx.channel().remoteAddress());
            RpcContext rpcContext = ChannelManager.getContextFromIdentified(ctx.channel());
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(ipAndPort + " to server channel inactive.");
            }
            if (null != rpcContext && null != rpcContext.getClientRole()) {
                rpcContext.release();
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("remove channel:" + ctx.channel() + "context:" + rpcContext);
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("remove unused channel:" + ctx.channel());
                }
            }
        }

        /**
         * Exception caught.
         *
         * @param ctx   the ctx
         * @param cause the cause
         * @throws Exception the exception
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("channel exx:" + cause.getMessage() + ",channel:" + ctx.channel());
            }
            ChannelManager.releaseRpcContext(ctx.channel());
            super.exceptionCaught(ctx, cause);
        }

        /**
         * User event triggered.
         *
         * @param ctx the ctx
         * @param evt the evt
         * @throws Exception the exception
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                debugLog("idle:" + evt);
                IdleStateEvent idleStateEvent = (IdleStateEvent) evt;
                if (idleStateEvent.state() == IdleState.READER_IDLE) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("channel:" + ctx.channel() + " read idle.");
                    }
                    handleDisconnect(ctx);
                    try {
                        closeChannelHandlerContext(ctx);
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage());
                    }
                }
            }
        }

    }

}
