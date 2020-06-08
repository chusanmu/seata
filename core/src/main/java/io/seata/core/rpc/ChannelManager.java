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

import io.netty.channel.Channel;
import io.seata.common.Constants;
import io.seata.common.exception.FrameworkException;
import io.seata.common.util.StringUtils;
import io.seata.core.protocol.IncompatibleVersionException;
import io.seata.core.protocol.RegisterRMRequest;
import io.seata.core.protocol.RegisterTMRequest;
import io.seata.core.protocol.Version;
import io.seata.core.rpc.netty.NettyPoolKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The type channel manager.
 * TODO: ChannelManager channel管理器，所有的channel在这里缓存着， 此类非常重要
 *
 * @author slievrly
 */
public class ChannelManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelManager.class);

    /**
     * TODO: 被识别的channel，简单来说就是已注册的channel，一个channel 对应着 一个rpcContext
     */
    private static final ConcurrentMap<Channel, RpcContext> IDENTIFIED_CHANNELS = new ConcurrentHashMap<>();

    /**
     * resourceId -> applicationId -> ip -> port -> RpcContext
     * TODO: 这里的数据结构 看起来比较长，这主要 可以看做是一个RM注册中心
     * resourceId对应RM id,
     * 一个resource 可以在一台机器上启动，可以同一个Ip 不同的端口号
     * 所以它的细粒度可以精细到一个IP的不同Port
     */
    private static final ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>>>> RM_CHANNELS = new ConcurrentHashMap<>();

    /**
     * ip+appname,port
     * TODO: TM的管理器，注册中心
     * 第一个key 为 ip+appname ，然后 port, 之后对应rpcContext
     */
    private static final ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>> TM_CHANNELS = new ConcurrentHashMap<>();

    /**
     * Is registered boolean.
     * TODO: 判断channel是否注册过
     *
     * @param channel the channel
     * @return the boolean
     */
    public static boolean isRegistered(Channel channel) {
        return IDENTIFIED_CHANNELS.containsKey(channel);
    }

    /**
     * Gets get role from channel.
     * TODO: 通过channel 获得 客户端的角色
     *
     * @param channel the channel
     * @return the get role from channel
     */
    public static NettyPoolKey.TransactionRole getRoleFromChannel(Channel channel) {
        if (IDENTIFIED_CHANNELS.containsKey(channel)) {
            return IDENTIFIED_CHANNELS.get(channel).getClientRole();
        }
        return null;
    }

    /**
     * Gets get context from identified.
     * TODO: 通过channel 拿到rpc context
     *
     * @param channel the channel
     * @return the get context from identified
     */
    public static RpcContext getContextFromIdentified(Channel channel) {
        return IDENTIFIED_CHANNELS.get(channel);
    }

    /**
     * TODO: 应用ID + : + 客户端远程地址 构成 客户端Id
     *
     * @param applicationId
     * @param channel
     * @return
     */
    private static String buildClientId(String applicationId, Channel channel) {
        return applicationId + Constants.CLIENT_ID_SPLIT_CHAR + ChannelUtil.getAddressFromChannel(channel);
    }

    private static String[] readClientId(String clientId) {
        return clientId.split(Constants.CLIENT_ID_SPLIT_CHAR);
    }

    /**
     * TODO: 根据一系列信息，构建RpcContext
     *
     * @param clientRole
     * @param version
     * @param applicationId
     * @param txServiceGroup
     * @param dbkeys
     * @param channel
     * @return
     */
    private static RpcContext buildChannelHolder(NettyPoolKey.TransactionRole clientRole, String version, String applicationId,
                                                 String txServiceGroup, String dbkeys, Channel channel) {
        RpcContext holder = new RpcContext();
        // TODO: 设置客户端角色， tm or rm or server?
        holder.setClientRole(clientRole);
        // TODO: 设置版本号
        holder.setVersion(version);
        // TODO: 设置客户端ID
        holder.setClientId(buildClientId(applicationId, channel));
        // TODO: 设置应用ID
        holder.setApplicationId(applicationId);
        // TODO: 设置事务组
        holder.setTransactionServiceGroup(txServiceGroup);
        // TODO: 设置DB
        holder.addResources(dbKeytoSet(dbkeys));
        // TODO: 设置channel
        holder.setChannel(channel);
        return holder;
    }

    /**
     * Register tm channel.
     * TODO: 注册TM的channel啊
     *
     * @param request the request
     * @param channel the channel
     * @throws IncompatibleVersionException the incompatible version exception
     */
    public static void registerTMChannel(RegisterTMRequest request, Channel channel)
            throws IncompatibleVersionException {
        // TODO: 检查版本号，其实没做什么特别的事
        Version.checkVersion(request.getVersion());
        // TODO: 构建RPC context
        RpcContext rpcContext = buildChannelHolder(NettyPoolKey.TransactionRole.TMROLE, request.getVersion(),
                request.getApplicationId(),
                request.getTransactionServiceGroup(),
                null, channel);
        // TODO: 这句代码很有意思，新构建的rpcContext持有了一份完整的 已注册channel
        rpcContext.holdInIdentifiedChannels(IDENTIFIED_CHANNELS);
        // TODO: 构建 client Id
        String clientIdentified = rpcContext.getApplicationId() + Constants.CLIENT_ID_SPLIT_CHAR
                + ChannelUtil.getClientIpFromChannel(channel);
        TM_CHANNELS.putIfAbsent(clientIdentified, new ConcurrentHashMap<>());
        // TODO: port, rpcContext
        ConcurrentMap<Integer, RpcContext> clientIdentifiedMap = TM_CHANNELS.get(clientIdentified);
        // TODO: rpcContext中持有clientIdentifiedMap
        rpcContext.holdInClientChannels(clientIdentifiedMap);
    }

    /**
     * Register rm channel.
     * TODO: 注册Rm的channel
     *
     * @param resourceManagerRequest the resource manager request
     * @param channel                the channel
     * @throws IncompatibleVersionException the incompatible  version exception
     */
    public static void registerRMChannel(RegisterRMRequest resourceManagerRequest, Channel channel)
            throws IncompatibleVersionException {
        // TODO: check version 啥事都没做
        Version.checkVersion(resourceManagerRequest.getVersion());
        // TODO: 拿到dbKey set
        Set<String> dbkeySet = dbKeytoSet(resourceManagerRequest.getResourceIds());
        RpcContext rpcContext;
        // TODO: 如果不包含当前channel, 进行构建rpcContext
        if (!IDENTIFIED_CHANNELS.containsKey(channel)) {
            // TODO: 注意这里的角色变成了RMROLE
            rpcContext = buildChannelHolder(NettyPoolKey.TransactionRole.RMROLE, resourceManagerRequest.getVersion(),
                    resourceManagerRequest.getApplicationId(), resourceManagerRequest.getTransactionServiceGroup(),
                    resourceManagerRequest.getResourceIds(), channel);
            // TODO: 设置当前rpcContext 持有 所有channel的一个引用
            rpcContext.holdInIdentifiedChannels(IDENTIFIED_CHANNELS);
        } else {
            // TODO: 否则说明已经注册过了，然后resource 添加DB就可以了
            rpcContext = IDENTIFIED_CHANNELS.get(channel);
            rpcContext.addResources(dbkeySet);
        }
        /* ---------------- 接下来就比较绕了 -------------- */
        // TODO: 如果dbKeySet 为空，直接返回，那还注册个毛的RM_CHANNELS, 这里resource，我们就当做是一个分支服务，对应的db，作为一个resourceId
        if (null == dbkeySet || dbkeySet.isEmpty()) {
            return;
        }
        for (String resourceId : dbkeySet) {
            String clientIp;
            // TODO: 这句话就高级了，一直往下拿，直到拿到port对应的RpcContext,
            ConcurrentMap<Integer, RpcContext> portMap = RM_CHANNELS.computeIfAbsent(resourceId, resourceIdKey -> new ConcurrentHashMap<>())
                    .computeIfAbsent(resourceManagerRequest.getApplicationId(), applicationId -> new ConcurrentHashMap<>())
                    .computeIfAbsent(clientIp = ChannelUtil.getClientIpFromChannel(channel), clientIpKey -> new ConcurrentHashMap<>());
            // TODO: rpcContext 内部持有 一份 resourceId 对应的map
            rpcContext.holdInResourceManagerChannels(resourceId, portMap);
            updateChannelsResource(resourceId, clientIp, resourceManagerRequest.getApplicationId());
        }

    }

    /**
     * TODO: 更新channelResource
     * <p>
     * 这里要做的主要就是，更新同一IP地址，也就是同一机器，不同Port的情况，当前机器不包括这个port，则添加进来啊
     *
     * @param resourceId
     * @param clientIp
     * @param applicationId
     */
    private static void updateChannelsResource(String resourceId, String clientIp, String applicationId) {
        // TODO: 拿到Port对应的RpcContext
        ConcurrentMap<Integer, RpcContext> sourcePortMap = RM_CHANNELS.get(resourceId).get(applicationId).get(clientIp);
        for (ConcurrentMap.Entry<String, ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer,
                RpcContext>>>> rmChannelEntry : RM_CHANNELS.entrySet()) {
            // TODO: 更新不同resource下的 clientIp -> port -> rpcContext
            if (rmChannelEntry.getKey().equals(resourceId)) {
                continue;
            }
            ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer,
                    RpcContext>>> applicationIdMap = rmChannelEntry.getValue();
            if (!applicationIdMap.containsKey(applicationId)) {
                continue;
            }
            ConcurrentMap<String, ConcurrentMap<Integer,
                    RpcContext>> clientIpMap = applicationIdMap.get(applicationId);
            if (!clientIpMap.containsKey(clientIp)) {
                continue;
            }
            ConcurrentMap<Integer, RpcContext> portMap = clientIpMap.get(clientIp);
            for (ConcurrentMap.Entry<Integer, RpcContext> portMapEntry : portMap.entrySet()) {
                Integer port = portMapEntry.getKey();
                // TODO: 如果当前机器 没有该port
                if (!sourcePortMap.containsKey(port)) {
                    // TODO: 拿到该 port对应的rpcContext
                    RpcContext rpcContext = portMapEntry.getValue();
                    // TODO: 维护进去，该机器 该port对应的rpcContext
                    sourcePortMap.put(port, rpcContext);
                    // 更新
                    rpcContext.holdInResourceManagerChannels(resourceId, port);
                }
            }
        }
    }

    private static Set<String> dbKeytoSet(String dbkey) {
        if (StringUtils.isNullOrEmpty(dbkey)) {
            return null;
        }
        return new HashSet<>(Arrays.asList(dbkey.split(Constants.DBKEYS_SPLIT_CHAR)));
    }

    /**
     * Release rpc context.
     *
     * @param channel the channel
     */
    public static void releaseRpcContext(Channel channel) {
        if (IDENTIFIED_CHANNELS.containsKey(channel)) {
            RpcContext rpcContext = getContextFromIdentified(channel);
            rpcContext.release();
        }
    }

    /**
     * Gets get same income client channel.
     * TODO: 获取相同的客户端channel
     *
     * @param channel the channel
     * @return the get same income client channel
     */
    public static Channel getSameClientChannel(Channel channel) {
        // TODO: 如果当前的channel是激活状态的，那就直接返回就ok了啊
        if (channel.isActive()) {
            return channel;
        }
        // TODO: 否则根据channel拿出来一个rpcContext
        RpcContext rpcContext = getContextFromIdentified(channel);
        // TODO: 如果rpcContext为空
        if (null == rpcContext) {
            LOGGER.error("rpcContext is null,channel:{},active:{}", channel, channel.isActive());
            return null;
        }
        // TODO: 再次检查当前channel是否是激活状态的，如果是的，就直接返回就Ok了
        if (rpcContext.getChannel().isActive()) {
            // recheck
            return rpcContext.getChannel();
        }
        // TODO: 从channel中拿出来客户端port
        Integer clientPort = ChannelUtil.getClientPortFromChannel(channel);
        // TODO: 再拿出来客户端角色
        NettyPoolKey.TransactionRole clientRole = rpcContext.getClientRole();
        // TODO: 如果是一个TM
        if (clientRole == NettyPoolKey.TransactionRole.TMROLE) {
            // TODO: 拿客户端ID
            String clientIdentified = rpcContext.getApplicationId() + Constants.CLIENT_ID_SPLIT_CHAR
                    + ChannelUtil.getClientIpFromChannel(channel);
            // TODO: 如果TM_CHANNELS中不包含当前客户度标识，直接返回null啊
            if (!TM_CHANNELS.containsKey(clientIdentified)) {
                return null;
            }
            // TODO: 拿出port对应 rpcContext map
            ConcurrentMap<Integer, RpcContext> clientRpcMap = TM_CHANNELS.get(clientIdentified);
            return getChannelFromSameClientMap(clientRpcMap, clientPort);
        } else if (clientRole == NettyPoolKey.TransactionRole.RMROLE) {
            // TODO: 如果是一个RM，resource manager, rpcContext.getClientRMHolderMap() ----> <resource, <port, rpcContext>>
            for (Map<Integer, RpcContext> clientRmMap : rpcContext.getClientRMHolderMap().values()) {
                Channel sameClientChannel = getChannelFromSameClientMap(clientRmMap, clientPort);
                // 如果sameClientChannel不为空，则直接返回
                if (null != sameClientChannel) {
                    return sameClientChannel;
                }
            }
        }
        return null;

    }

    /**
     * TODO: 此时 exclusivePort 对应的 channel 已经是不活跃的了
     *
     * @param clientChannelMap
     * @param exclusivePort
     * @return
     */
    private static Channel getChannelFromSameClientMap(Map<Integer, RpcContext> clientChannelMap, int exclusivePort) {
        if (null != clientChannelMap && !clientChannelMap.isEmpty()) {
            for (ConcurrentMap.Entry<Integer, RpcContext> entry : clientChannelMap.entrySet()) {
                // TODO: 所以这里，直接进行一个移除 不活跃连接
                if (entry.getKey() == exclusivePort) {
                    clientChannelMap.remove(entry.getKey());
                    continue;
                }
                // TODO: 同一机器，不同端口的获取channel
                Channel channel = entry.getValue().getChannel();
                if (channel.isActive()) {
                    return channel;
                }
                // TODO: 如果channel不是存活的，就移除了吧
                clientChannelMap.remove(entry.getKey());
            }
        }
        return null;
    }

    /**
     * Gets get channel.
     * TODO: 获得一个channel，根据resourceId, 可客户端Id
     *
     * @param resourceId Resource ID
     * @param clientId   Client ID - ApplicationId:IP:Port
     * @return Corresponding channel, NULL if not found.
     */
    public static Channel getChannel(String resourceId, String clientId) {
        Channel resultChannel = null;
        // TODO: 根据分隔符，切割，获得客户端信息
        String[] clientIdInfo = readClientId(clientId);
        // TODO: 为空，或者是长度不符合，直接抛异常啊
        if (clientIdInfo == null || clientIdInfo.length != 3) {
            throw new FrameworkException("Invalid Client ID: " + clientId);
        }
        /* ---------------- 三个东西全部拿到 -------------- */

        //  客户端Id
        String targetApplicationId = clientIdInfo[0];
        // 目标Ip
        String targetIP = clientIdInfo[1];
        // 目标端口
        int targetPort = Integer.parseInt(clientIdInfo[2]);

        ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer,
                RpcContext>>> applicationIdMap = RM_CHANNELS.get(resourceId);

        // TODO: 如果有一个为空，直接返回null
        if (targetApplicationId == null || applicationIdMap == null || applicationIdMap.isEmpty()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("No channel is available for resource[{}]", resourceId);
            }
            return null;
        }

        // TODO: applicationId --> 获得ipMap
        ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>> ipMap = applicationIdMap.get(targetApplicationId);

        // TODO: ipMap不为空，才能继续往下走啊
        if (null != ipMap && !ipMap.isEmpty()) {

            // Firstly, try to find the original channel through which the branch was registered.
            // TODO: 根据ip获得 一个 <port, rpcContext>
            ConcurrentMap<Integer, RpcContext> portMapOnTargetIP = ipMap.get(targetIP);
            // TODO: 如果不为空
            if (portMapOnTargetIP != null && !portMapOnTargetIP.isEmpty()) {
                // 拿到rpcContext
                RpcContext exactRpcContext = portMapOnTargetIP.get(targetPort);
                if (exactRpcContext != null) {
                    // TODO: 取出channel
                    Channel channel = exactRpcContext.getChannel();
                    if (channel.isActive()) {
                        // 如果当前channel是激活的，则赋值给resultChannel
                        resultChannel = channel;
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Just got exactly the one {} for {}", channel, clientId);
                        }
                    } else {
                        // TODO: 不是active状态的了，直接移除吧
                        if (portMapOnTargetIP.remove(targetPort, exactRpcContext)) {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Removed inactive {}", channel);
                            }
                        }
                    }
                }

                // The original channel was broken, try another one.
                // TODO: 如果resultChannel为空，目标端口的channel不是激活状态的了，那就换一个该机器上的其它端口
                if (resultChannel == null) {
                    for (ConcurrentMap.Entry<Integer, RpcContext> portMapOnTargetIPEntry : portMapOnTargetIP
                            .entrySet()) {
                        // TODO: 获得channel,如果channel是激活状态的，给resultChannel赋值，然后break掉 就好了
                        Channel channel = portMapOnTargetIPEntry.getValue().getChannel();

                        if (channel.isActive()) {
                            resultChannel = channel;
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info(
                                        "Choose {} on the same IP[{}] as alternative of {}", channel, targetIP, clientId);
                            }
                            break;
                        } else {
                            // TODO: 如果不是激活状态的了，那就移除就好了
                            if (portMapOnTargetIP.remove(portMapOnTargetIPEntry.getKey(),
                                    portMapOnTargetIPEntry.getValue())) {
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info("Removed inactive {}", channel);
                                }
                            }
                        }
                    }
                }
            }

            // No channel on the this app node, try another one.
            // TODO: 如果resultChannel还是为空, 换个ip继续查
            if (resultChannel == null) {
                for (ConcurrentMap.Entry<String, ConcurrentMap<Integer, RpcContext>> ipMapEntry : ipMap
                        .entrySet()) {
                    // 如果是目标ip，直接跳过呗，刚才查活跃连接了，没查到
                    if (ipMapEntry.getKey().equals(targetIP)) {
                        continue;
                    }
                    // 拿到该ip对应的port集合
                    ConcurrentMap<Integer, RpcContext> portMapOnOtherIP = ipMapEntry.getValue();
                    // 如果port集合为空，则再换一个ip就好了
                    if (portMapOnOtherIP == null || portMapOnOtherIP.isEmpty()) {
                        continue;
                    }
                    // 遍历 <port, rpcContext>
                    for (ConcurrentMap.Entry<Integer, RpcContext> portMapOnOtherIPEntry : portMapOnOtherIP.entrySet()) {
                        Channel channel = portMapOnOtherIPEntry.getValue().getChannel();
                        // 如果channel是激活状态，将channel赋值给resultChannel
                        if (channel.isActive()) {
                            resultChannel = channel;
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Choose {} on the same application[{}] as alternative of {}", channel, targetApplicationId, clientId);
                            }
                            break;
                        } else {
                            // 一样，如果处于不活跃状态，直接移除就好了,
                            if (portMapOnOtherIP.remove(portMapOnOtherIPEntry.getKey(),
                                    portMapOnOtherIPEntry.getValue())) {
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info("Removed inactive {}", channel);
                                }
                            }
                        }
                    }
                    // 拿到resultChannel不为空，则break 就好了
                    if (resultChannel != null) {
                        break;
                    }
                }
            }
        }
        // TODO: 如果resultChannel为空，换个application继续查
        if (resultChannel == null) {
            resultChannel = tryOtherApp(applicationIdMap, targetApplicationId);

            if (resultChannel == null) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("No channel is available for resource[{}] as alternative of {}", resourceId, clientId);
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Choose {} on the same resource[{}] as alternative of {}", resultChannel, resourceId, clientId);
                }
            }
        }

        return resultChannel;

    }

    /**
     * applicationIdMap ----->  <applicationId,<Ip, <port,rpcContext>>>
     *
     * @param applicationIdMap
     * @param myApplicationId
     * @return
     */
    private static Channel tryOtherApp(ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Integer,
            RpcContext>>> applicationIdMap, String myApplicationId) {
        Channel chosenChannel = null;
        for (ConcurrentMap.Entry<String, ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>>> applicationIdMapEntry : applicationIdMap
                .entrySet()) {
            // TODO: 换个不同的applicationId继续，如果是当前的就不查了
            if (!StringUtils.isNullOrEmpty(myApplicationId) && applicationIdMapEntry.getKey().equals(myApplicationId)) {
                continue;
            }

            ConcurrentMap<String, ConcurrentMap<Integer, RpcContext>> targetIPMap = applicationIdMapEntry.getValue();
            // 为空了直接 continue就好了 下面的处理逻辑其实都差不多
            if (targetIPMap == null || targetIPMap.isEmpty()) {
                continue;
            }

            for (ConcurrentMap.Entry<String, ConcurrentMap<Integer, RpcContext>> targetIPMapEntry : targetIPMap
                    .entrySet()) {
                ConcurrentMap<Integer, RpcContext> portMap = targetIPMapEntry.getValue();
                if (portMap == null || portMap.isEmpty()) {
                    continue;
                }

                for (ConcurrentMap.Entry<Integer, RpcContext> portMapEntry : portMap.entrySet()) {
                    Channel channel = portMapEntry.getValue().getChannel();
                    // 如果channel是激活的
                    if (channel.isActive()) {
                        chosenChannel = channel;
                        break;
                    } else {
                        // 否则移除不活跃的channel
                        if (portMap.remove(portMapEntry.getKey(), portMapEntry.getValue())) {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Removed inactive {}", channel);
                            }
                        }
                    }
                }
                if (chosenChannel != null) {
                    break;
                }
            }
            if (chosenChannel != null) {
                break;
            }
        }
        return chosenChannel;

    }

    /**
     * get rm channels
     * TODO: 获得rm channel
     *
     * @return
     */
    public static Map<String, Channel> getRmChannels() {
        // TODO: 如果rm_channel为空 直接返回null
        if (RM_CHANNELS.isEmpty()) {
            return null;
        }
        Map<String, Channel> channels = new HashMap<>(RM_CHANNELS.size());
        for (String resourceId : RM_CHANNELS.keySet()) {
            Channel channel = tryOtherApp(RM_CHANNELS.get(resourceId), null);
            if (channel == null) {
                continue;
            }
            // TODO: <resourceId,channle>
            channels.put(resourceId, channel);
        }
        return channels;
    }
}
