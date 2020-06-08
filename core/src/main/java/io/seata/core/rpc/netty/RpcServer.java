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

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

import io.netty.channel.Channel;
import io.seata.core.protocol.HeartbeatMessage;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.rpc.ChannelManager;
import io.seata.core.rpc.DefaultServerMessageListenerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Abstract rpc server.
 *
 * @author slievrly
 */
public class RpcServer extends AbstractRpcRemotingServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcServer.class);


    /**
     * Instantiates a new Abstract rpc server.
     *
     * @param messageExecutor the message executor
     */
    public RpcServer(ThreadPoolExecutor messageExecutor) {
        // TODO: 把处理消息线程池，传过去
        super(messageExecutor, new NettyServerConfig());
    }

    /**
     * Init.
     */
    @Override
    public void init() {
        // TODO: new 一个RPC消息处理器，默认的消息处理器
        DefaultServerMessageListenerImpl defaultServerMessageListenerImpl =
            new DefaultServerMessageListenerImpl(getTransactionMessageHandler());
        // TODO: 会去初始化，打印日志
        defaultServerMessageListenerImpl.init();
        // TODO: 把它自己(RpcServer) 当成ServerMessageSender set进去
        defaultServerMessageListenerImpl.setServerMessageSender(this);
        // TODO: 设置messageListener
        super.setServerMessageListener(defaultServerMessageListenerImpl);
        // TODO: 设置channelHandlers
        super.setChannelHandlers(new ServerHandler());
        // TODO: 开始初始化，从父类开始初始化
        super.init();
    }

    /**
     * Destroy.
     */
    @Override
    public void destroy() {
        super.destroy();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("destroyed rpcServer");
        }
    }



    /**
     * Send response.
     * rm reg,rpc reg,inner response
     * TODO: 发送响应
     * @param request the request
     * @param channel the channel
     * @param msg     the msg
     */
    @Override
    public void sendResponse(RpcMessage request, Channel channel, Object msg) {
        Channel clientChannel = channel;
        // TODO: 如果不是检查心跳的message, 则查出clientChannel
        if (!(msg instanceof HeartbeatMessage)) {
            clientChannel = ChannelManager.getSameClientChannel(channel);
        }
        // TODO: clientChannel不为空
        if (clientChannel != null) {
            super.defaultSendResponse(request, clientChannel, msg);
        } else {
            // TODO: 为空抛异常, 这里异常信息可以更改一下 clientChannel is null.
            throw new RuntimeException("channel is error. channel:" + clientChannel);
        }
    }

    /**
     * Send request with response object.
     * send syn request for rm
     * TODO: 发送同步请求，实际上是可以拿到返回值得异步请求..
     * 这里是server向client发请求
     * @param resourceId the db key
     * @param clientId   the client ip
     * @param message    the message
     * @param timeout    the timeout
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendSyncRequest(String resourceId, String clientId, Object message,
        long timeout) throws TimeoutException {
        // TODO: 根据resourceId和clientId 拿出一个clientChannel
        Channel clientChannel = ChannelManager.getChannel(resourceId, clientId);
        // 为空则抛异常
        if (clientChannel == null) {
            throw new RuntimeException("rm client is not connected. dbkey:" + resourceId
                + ",clientId:" + clientId);
        }
        // 向clientChannel发送一个带有响应的request, timeout默认3秒
        return sendAsyncRequestWithResponse(null, clientChannel, message, timeout);
    }

    /**
     * Send request with response object.
     * send syn request for rm
     * 会使用默认的超时时间
     * @param clientChannel the client channel
     * @param message       the message
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendSyncRequest(Channel clientChannel, Object message) throws TimeoutException {
        return sendSyncRequest(clientChannel, message, NettyServerConfig.getRpcRequestTimeout());
    }

    /**
     * Send request with response object.
     * send syn request for rm
     *
     * @param clientChannel the client channel
     * @param message       the message
     * @param timeout       the timeout
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendSyncRequest(Channel clientChannel, Object message, long timeout) throws TimeoutException {
        // TODO: 如果clientChannel为空，则直接抛异常
        if (clientChannel == null) {
            throw new RuntimeException("rm client is not connected");

        }
        // TODO: 发送请求，会等待响应
        return sendAsyncRequestWithResponse(null, clientChannel, message, timeout);
    }

    /**
     * Send request with response object.
     * 重载方法，发送同步请求，参数比较全
     * @param resourceId the db key
     * @param clientId   the client ip
     * @param message    the msg
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendSyncRequest(String resourceId, String clientId, Object message)
        throws TimeoutException {
        return sendSyncRequest(resourceId, clientId, message, NettyServerConfig.getRpcRequestTimeout());
    }

    /**
     * Send request with response object.
     *
     * @param channel   the channel
     * @param message    the msg
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendASyncRequest(Channel channel, Object message) throws TimeoutException {
        return sendAsyncRequestWithoutResponse(channel, message);
    }
}