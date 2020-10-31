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
package io.seata.core.protocol;

import io.seata.common.exception.ShouldNeverHappenException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The type Message future.
 *
 * @author slievrly
 */
public class MessageFuture {
    /**
     * 对应的 rpc消息
     */
    private RpcMessage requestMessage;
    /**
     * 超时时间
     */
    private long timeout;

    /**
     * 开始时间
     */
    private long start = System.currentTimeMillis();

    /**
     * 用来存放执行结果
     */
    private transient CompletableFuture<Object> origin = new CompletableFuture<>();

    /**
     * TODO：判断是否超时了
     * Is timeout boolean.
     *
     * @return the boolean
     */
    public boolean isTimeout() {
        return System.currentTimeMillis() - start > timeout;
    }

    /**
     * TODO: 这个很重要，获取执行结果, 这个方法依赖completableFuture，它是阻塞的
     *
     * Get object.
     *
     * @param timeout the timeout
     * @param unit    the unit
     * @return the object
     * @throws TimeoutException the timeout exception
     * @throws InterruptedException the interrupted exception
     */
    public Object get(long timeout, TimeUnit unit) throws TimeoutException,
        InterruptedException {
        Object result = null;
        try {
            // TODO: 获取结果
            result = origin.get(timeout, unit);
        } catch (ExecutionException e) {
            throw new ShouldNeverHappenException("Should not get results in a multi-threaded environment", e);
        } catch (TimeoutException e) {
            throw new TimeoutException("cost " + (System.currentTimeMillis() - start) + " ms");
        }

        // TODO: 看看结果是否是个runtimeException，如果是个异常，直接就抛出
        if (result instanceof RuntimeException) {
            throw (RuntimeException)result;
        } else if (result instanceof Throwable) {
            // TODO: 如果是throwable类型的异常 也包成runtimeException返回回去
            throw new RuntimeException((Throwable)result);
        }

        // TODO: 最后把结果返回回去
        return result;
    }

    /**
     * Sets result message.
     *
     * @param obj the obj
     */
    public void setResultMessage(Object obj) {
        origin.complete(obj);
    }

    /**
     * Gets request message.
     *
     * @return the request message
     */
    public RpcMessage getRequestMessage() {
        return requestMessage;
    }

    /**
     * Sets request message.
     *
     * @param requestMessage the request message
     */
    public void setRequestMessage(RpcMessage requestMessage) {
        this.requestMessage = requestMessage;
    }

    /**
     * Gets timeout.
     *
     * @return the timeout
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Sets timeout.
     *
     * @param timeout the timeout
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}
