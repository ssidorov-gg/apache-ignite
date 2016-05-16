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

package org.apache.ignite.internal;

import java.util.Collection;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests kernal stop while it is being accessed from EntryProcessor.
 */
public class GridKernalConcurrentEntryProcessorAccessStopSelfTest extends GridCommonAbstractTest {
    private Thread invoker;

    /**
     * Default constructor.
     */
    public GridKernalConcurrentEntryProcessorAccessStopSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        invoker.start();
        stopGrid();
    }

    /**
     * Tests concurrent instance shutdown.
     */
    public void testConcurrentAccess() throws Exception {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        Ignite ignite = grid();

        IgniteCache<Object, Object> dfltCache = ignite.getOrCreateCache(ccfg);

        dfltCache.put("1", "1");

        invoker = new Thread(new Runnable() {
            @Override public void run() {

                dfltCache.invoke("1", new EntryProcessor<Object, Object, Object>() {
                    @Override
                    public Object process(MutableEntry<Object, Object> entry,
                        Object... arguments) throws EntryProcessorException {

                        int i = 100000;
                        while(i-->=0)
                            grid().cluster().nodes();

                        entry.remove();

                        return null;
                    }
                });
            }
        });
    }
}
