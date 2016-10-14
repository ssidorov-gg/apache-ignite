/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Objects;
import java.util.concurrent.Callable;
import javax.cache.processor.MutableEntry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 */
public class IgniteCachePartitionedBackupNodeFailureRecoveryTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc}*/
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc}*/
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc}*/
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /** {@inheritDoc}*/
    @Override protected CacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return CacheAtomicWriteOrderMode.PRIMARY;
    }

    /** {@inheritDoc}*/
    @Override protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /** {@inheritDoc}*/
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cacheCfg = super.cacheConfiguration(gridName);

        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        return cacheCfg;
    }

    /**
     * Test stop and restart backup node.
     */
    public void testBackUpFail() throws Exception {
        final IgniteEx node1 = grid(0);
        final IgniteEx node2 = grid(1);
        final IgniteEx node3 = grid(2);

        final IgniteCache<Integer, Integer> cache1 = node1.cache(null);

        Affinity<Integer> aff = node1.affinity(null);

        Integer key0 = null;

        for (int key = 0; key < 10_000; key++) {
            if (aff.isPrimary(node2.cluster().localNode(), key) && aff.isBackup(node3.cluster().localNode(), key)) {
                key0 = key;

                break;
            }
        }

        assert key0 != null;

        final Integer val = 0;

        cache1.put(key0, val);

        final AtomicBoolean finished = new AtomicBoolean();

        final ReentrantLock lock = new ReentrantLock();

        final AtomicInteger cntr = new AtomicInteger();

        final Integer finalKey = key0;

        IgniteInternalFuture<Void> primaryFut = runAsync(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                while (!finished.get()) {
                    lock.lock();
                    try {
                        cache1.invoke(finalKey, new EntryProcessor<Integer, Integer, Void>() {
                            @Override
                            public Void process(MutableEntry<Integer, Integer> entry, Object... arguments) throws EntryProcessorException {
                                Integer v = entry.getValue() + 1;
                                entry.setValue(v);
                                return null;
                            }
                        });

                        cntr.getAndIncrement();
                    } finally {
                        lock.unlock();
                    }
                }

                return null;
            }
        });

        IgniteInternalFuture<Void> backupFut = runAsync(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                while (!finished.get()) {
                   stopGrid(2);

                    lock.lock();

                    try {
                        IgniteEx backUp = startGrid(2);

                        IgniteCache<Integer, Integer> cache3 = backUp.cache(null);

                        Integer backUpVal = cache3.localPeek(finalKey, CachePeekMode.BACKUP);

                        assert Objects.equals(backUpVal, (val + cntr.get()));
                    } finally {
                        lock.unlock();
                    }
                }
                return null;
            }
        });

        Thread.sleep(30_000);

        finished.set(true);

        primaryFut.get();
        backupFut.get();

        Assert.assertTrue(primaryFut.error() == null);
        Assert.assertTrue(backupFut.error() == null);
    }
}
