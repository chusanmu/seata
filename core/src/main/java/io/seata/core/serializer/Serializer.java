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
package io.seata.core.serializer;

/**
 * The interface Codec.
 * 序列化接口
 *
 * @author zhangsen
 */
public interface Serializer {

    /**
     * Encode object to byte[].
     *
     * @param <T> the type parameter
     * @param t   the t
     * @return the byte [ ]
     * 序列化，对象转字节
     */
    <T> byte[] serialize(T t);

    /**
     * Decode t from byte[].
     *
     * @param <T>   the type parameter
     * @param bytes the bytes
     * @return the t
     * 字节转成对象
     */
    <T> T deserialize(byte[] bytes);
}
