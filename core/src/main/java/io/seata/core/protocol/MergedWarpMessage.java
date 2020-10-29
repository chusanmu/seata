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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The type Merged warp message.
 * TODO: Merged 的消息啊，可能会把多条消息拧成一条消息，然后就是这个消息类型
 *
 * @author slievrly
 */
public class MergedWarpMessage extends AbstractMessage implements Serializable, MergeMessage {

    /**
     * The Msgs.
     */
    public List<AbstractMessage> msgs = new ArrayList<>();
    /**
     * The Msg ids.
     */
    public List<Integer> msgIds = new ArrayList<>();

    @Override
    public short getTypeCode() {
        return MessageType.TYPE_SEATA_MERGE;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SeataMergeMessage ");
        for (AbstractMessage msg : msgs) {
            sb.append(msg.toString()).append("\n");
        }
        return sb.toString();
    }
}
