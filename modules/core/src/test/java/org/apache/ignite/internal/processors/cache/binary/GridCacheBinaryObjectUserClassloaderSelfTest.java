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

package org.apache.ignite.internal.processors.cache.binary;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridCacheBinaryObjectUserClassloaderSelfTest extends GridCommonAbstractTest {
    /** */
    private static volatile boolean customBinaryConf = false;

    /** */
    private static volatile boolean deserialized = false;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setClassLoader(getExternalClassLoader());

        if (customBinaryConf) {
            BinaryTypeConfiguration btcfg = new BinaryTypeConfiguration();

            btcfg.setTypeName("org.apache.ignite.tests.p2p.CacheDeploymentTestValue3");

            btcfg.setSerializer(new BinarySerializer() {
                /** {@inheritDoc} */
                @Override public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {
                    //No-op.
                }

                /** {@inheritDoc} */
                @Override public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {
                    deserialized = true;
                }
            });

            BinaryConfiguration bcfg = new BinaryConfiguration();

            bcfg.setTypeConfigurations(Collections.singletonList(btcfg));

            cfg.setBinaryConfiguration(bcfg);
        }

        return cfg;
    }

    /**
     * Gets cache configuration for grid with specified name.
     *
     * @param gridName Grid name.
     * @return Cache configuration.
     */
    CacheConfiguration cacheConfiguration(String gridName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        return cacheCfg;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testConfigurationRegistration() throws Exception {
        try {
            customBinaryConf = true;

            Ignite i1 = startGrid(1);
            Ignite i2 = startGrid(2);

            IgniteCache<Integer, Object> cache1 = i1.cache(null);
            IgniteCache<Integer, Object> cache2 = i2.cache(null);

            ClassLoader ldr = i1.configuration().getClassLoader();

            Object v1 = ldr.loadClass("org.apache.ignite.tests.p2p.CacheDeploymentTestValue3").newInstance();

            cache1.put(1, v1);

            deserialized = false;

            cache2.get(1);

            assertTrue(deserialized);
        }
        finally {
            customBinaryConf = false;
        }
    }
}
