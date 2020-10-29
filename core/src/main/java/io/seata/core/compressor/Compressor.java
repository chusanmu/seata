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
package io.seata.core.compressor;

/**
 *
 * 编码压缩的顶级接口
 * @author jsbxyyx
 */
public interface Compressor {

    /**
     * compress byte[] to byte[].
     * @param bytes the bytes
     * @return the byte[]
     * 压缩，压缩肯定是进去是字节，出来也是字节
     */
    byte[] compress(byte[] bytes);

    /**
     * decompress byte[] to byte[].
     * @param bytes the bytes
     * @return the byte[]
     * 解压，出来字节，进去的字节长度 肯定是小于 出来的字节长度的
     */
    byte[] decompress(byte[] bytes);

}
