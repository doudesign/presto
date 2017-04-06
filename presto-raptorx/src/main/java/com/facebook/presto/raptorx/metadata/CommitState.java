/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptorx.metadata;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Arrays.asList;

public enum CommitState
{
    COMMITTING(1),
    COMMITTED(2),
    ROLLING_BACK(3),
    AWAITING_CLEANUP(4);

    private final int id;

    private static final Map<Integer, CommitState> IDS = uniqueIndex(asList(values()), CommitState::id);

    CommitState(int id)
    {
        this.id = id;
    }

    public int id()
    {
        return id;
    }

    public static CommitState fromId(int id)
    {
        CommitState state = IDS.get(id);
        checkArgument(state != null, "Invalid ID: %s", id);
        return state;
    }
}
