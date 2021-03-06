/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;

import java.util.*;

/**
 * Tests transaction consistency when originating node fails.
 */
public class GridCachePartitionedTxOriginatingNodeFailureSelfTest extends
    IgniteTxOriginatingNodeFailureAbstractSelfTest {
    /** */
    private static final int BACKUP_CNT = 2;

    /** {@inheritDoc} */
    @Override protected CacheDistributionMode distributionMode() {
        return CacheDistributionMode.NEAR_PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setBackups(BACKUP_CNT);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected Class<?> ignoreMessageClass() {
        return GridNearTxPrepareRequest.class;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxFromPrimary() throws Exception {
        GridCacheAdapter<Integer, String> cache = ((IgniteKernal)grid(originatingNode())).internalCache();

        ClusterNode txNode = grid(originatingNode()).localNode();

        Integer key = null;

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (cache.affinity().isPrimary(txNode, i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        testTxOriginatingNodeFails(Collections.singleton(key), false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxFromBackup() throws Exception {
        GridCacheAdapter<Integer, String> cache = ((IgniteKernal)grid(originatingNode())).internalCache();

        ClusterNode txNode = grid(originatingNode()).localNode();

        Integer key = null;

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (cache.affinity().isBackup(txNode, i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        testTxOriginatingNodeFails(Collections.singleton(key), false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxFromNotColocated() throws Exception {
        GridCacheAdapter<Integer, String> cache = ((IgniteKernal)grid(originatingNode())).internalCache();

        ClusterNode txNode = grid(originatingNode()).localNode();

        Integer key = null;

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (!cache.affinity().isPrimary(txNode, i) && !cache.affinity().isBackup(txNode, i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        testTxOriginatingNodeFails(Collections.singleton(key), false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxAllNodes() throws Exception {
        GridCacheAdapter<Integer, String> cache = ((IgniteKernal)grid(originatingNode())).internalCache();

        List<ClusterNode> allNodes = new ArrayList<>(GRID_CNT);

        for (int i = 0; i < GRID_CNT; i++)
            allNodes.add(grid(i).localNode());

        Collection<Integer> keys = new ArrayList<>();

        for (int i = 0; i < Integer.MAX_VALUE && !allNodes.isEmpty(); i++) {
            for (Iterator<ClusterNode> iter = allNodes.iterator(); iter.hasNext();) {
                ClusterNode node = iter.next();

                if (cache.affinity().isPrimary(node, i)) {
                    keys.add(i);

                    iter.remove();

                    break;
                }
            }
        }

        assertEquals(GRID_CNT, keys.size());

        testTxOriginatingNodeFails(keys, false);
    }
}
