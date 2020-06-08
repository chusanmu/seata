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
package io.seata.discovery.loadbalance;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.seata.common.loader.LoadLevel;

/**
 * The type Round robin load balance.
 * TODO: 轮询算法，优先级也比较高
 *
 * @author slievrly
 */
@LoadLevel(name = "RoundRobinLoadBalance", order = 1)
public class RoundRobinLoadBalance extends AbstractLoadBalance {

    private final AtomicInteger sequence = new AtomicInteger();

    @Override
    protected <T> T doSelect(List<T> invokers) {
        int length = invokers.size();
        // TODO: seata优化点，这地方可以进行一个优化，如果 getPositiveSequence() 是2的幂，可以直接进行位运算
        int positiveSequence = getPositiveSequence();

        if (isPowerOfTwo(length)) {
            return invokers.get(positiveSequence & (length - 1));
        } else {
            return invokers.get(positiveSequence % length);
        }
    }

    private static boolean isPowerOfTwo(int val) {
        return (val & -val) == val;
    }

    private int getPositiveSequence() {
        for (; ; ) {
            int current = sequence.get();
            int next = current >= Integer.MAX_VALUE ? 0 : current + 1;
            if (sequence.compareAndSet(current, next)) {
                return current;
            }
        }
    }

}
