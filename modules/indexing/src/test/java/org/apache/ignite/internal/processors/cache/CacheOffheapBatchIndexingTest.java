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

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests various cache operations with indexing enabled.
 * TODO FIXME needs cleanup.
 */
public class CacheOffheapBatchIndexingTest extends GridCommonAbstractTest {
    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Loading date into cache
     * @param name
     */
    private void preload(String name) {
        try (IgniteDataStreamer<Object, Object> streamer = ignite(0).dataStreamer(name)) {
            for (int i = 0; i < 30_000; i++) {
                if (i % 2 == 0)
                    streamer.addData(i, new Person(i, i + 1, String.valueOf(i), String.valueOf(i + 1), salary(i)));
                else
                    streamer.addData(i, new Organization(i, String.valueOf(i)));
            }
        }
    }

    /**
     * Test removal using EntryProcessor.
     * @throws Exception If failed.
     */
    public void testBatchRemove() throws Exception {
        Ignite ignite = grid(0);

        final IgniteCache<Object, Object> cache =
            ignite.createCache(cacheConfiguration(1, new Class<?>[] {Integer.class, Organization.class}));

        try {
            int iterations = 50;
            while (iterations-- >= 0) {
                int total = 1000;

                for (int id = 0; id < total; id++)
                    cache.put(id, new Organization(id, "Organization " + id));

                cache.invoke(0, new CacheEntryProcessor<Object, Object, Object>() {
                    @Override public Object process(MutableEntry<Object, Object> entry,
                        Object... arguments) throws EntryProcessorException {

                        entry.remove();

                        return null;
                    }
                });

                QueryCursor<List<?>> q = cache.query(new SqlFieldsQuery("select _key,_val from Organization where id=0"));

                assertEquals(0, q.getAll().size());

                q = cache.query(new SqlFieldsQuery("select _key,_val from Organization where id=1"));

                assertEquals(1, q.getAll().size());

                assertEquals(total - 1, cache.size());

                cache.clear();
            }
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * Test putAll with multiple indexed entites and streamer pre-loading with low off-heap cache size.
     * The test fails in remove call.
     */
    public void testPutAllMultupleEntitiesAndStreamer() {
        //fail("IGNITE-2982");
        doStreamerBatchTest(50, 1_000, new Class<?>[] {Integer.class, Person.class, Integer.class, Organization.class}, 1, true);
    }

    /**
     * Test putAll with multiple indexed entites and streamer preloading with default off-heap cache size.
     * The test fails on remove and often hangs in putAll call.
     */
    public void testPutAllMultupleEntitiesAndStreamerDfltOffHeapRowCacheSize() {
        //fail("IGNITE-2982");
        doStreamerBatchTest(50, 1_000, new Class<?>[] {Integer.class, Person.class, Integer.class, Organization.class},
            CacheConfiguration.DFLT_SQL_ONHEAP_ROW_CACHE_SIZE, true);
    }

    /**
     * Test putAll after with streamer batch load with one entity.
     * The test fails in putAll.
     */
    public void testPuAllSingleEntity() {
        //fail("IGNITE-2982");
        doStreamerBatchTest(50, 1_000, new Class<?>[] {Integer.class, Organization.class}, 1, false);
    }

    /**
     * Test putAll after with streamer batch load with one entity.
     */
    private void doStreamerBatchTest(int iterations, int entitiesCnt, Class<?>[] entityClasses, int onHeapRowCacheSize, boolean preloadInStreamer) {
        Ignite ignite = grid(0);

        final IgniteCache<Object, Object> cache =
            ignite.createCache(cacheConfiguration(onHeapRowCacheSize, entityClasses));

        try {
            if (preloadInStreamer)
                preload(cache.getName());

            while (iterations-- >= 0) {
                Map<Integer, Person> putMap1 = new TreeMap<>();

                for (int i = 0; i < entitiesCnt; i++)
                    putMap1.put(i, new Person(i, i + 1, String.valueOf(i), String.valueOf(i + 1), salary(i)));

                cache.putAll(putMap1);

                Map<Integer, Organization> putMap2 = new TreeMap<>();

                for (int i = entitiesCnt / 2; i < entitiesCnt * 3 / 2; i++) {
                    cache.remove(i);

                    putMap2.put(i, new Organization(i, String.valueOf(i)));
                }

                cache.putAll(putMap2);
            }
        } finally {
            cache.destroy();
        }
    }

    /**
     * @param base Base.
     */
    private double salary(int base) {
        return base * 100.;
    }

    /**
     * @param onHeapRowCacheSize on heap row cache size.
     * @param indexedTypes indexed types for cache.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(int onHeapRowCacheSize, Class<?>[] indexedTypes) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setMemoryMode(OFFHEAP_TIERED);
        ccfg.setSqlOnheapRowCacheSize(onHeapRowCacheSize);
        ccfg.setIndexedTypes(indexedTypes);

        return ccfg;
    }

    /**
     * Ignite cache value class.
     */
    private static class Person implements Binarylizable {

        /** Person ID. */
        @QuerySqlField(index = true)
        private int id;

        /** Organization ID. */
        @QuerySqlField(index = true)
        private int orgId;

        /** First name (not-indexed). */
        @QuerySqlField
        private String firstName;

        /** Last name (not indexed). */
        @QuerySqlField
        private String lastName;

        /** Salary. */
        @QuerySqlField(index = true)
        private double salary;

        /**
         * Constructs empty person.
         */
        public Person() {
            // No-op.
        }

        /**
         * Constructs person record.
         *
         * @param id Person ID.
         * @param orgId Organization ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param salary Salary.
         */
        public Person(int id, int orgId, String firstName, String lastName, double salary) {
            this.id = id;
            this.orgId = orgId;
            this.firstName = firstName;
            this.lastName = lastName;
            this.salary = salary;
        }

        /**
         * @return Person id.
         */
        public int getId() {
            return id;
        }

        /**
         * @param id Person id.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         * @return Organization id.
         */
        public int getOrganizationId() {
            return orgId;
        }

        /**
         * @param orgId Organization id.
         */
        public void setOrganizationId(int orgId) {
            this.orgId = orgId;
        }

        /**
         * @return Person first name.
         */
        public String getFirstName() {
            return firstName;
        }

        /**
         * @param firstName Person first name.
         */
        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        /**
         * @return Person last name.
         */
        public String getLastName() {
            return lastName;
        }

        /**
         * @param lastName Person last name.
         */
        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        /**
         * @return Salary.
         */
        public double getSalary() {
            return salary;
        }

        /**
         * @param salary Salary.
         */
        public void setSalary(double salary) {
            this.salary = salary;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("id", id);
            writer.writeInt("orgId", orgId);
            writer.writeString("firstName", firstName);
            writer.writeString("lastName", lastName);
            writer.writeDouble("salary", salary);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            id = reader.readInt("id");
            orgId = reader.readInt("orgId");
            firstName = reader.readString("firstName");
            lastName = reader.readString("lastName");
            salary = reader.readDouble("salary");
        }
    }

    /**
     * Ignite cache value class with indexed field.
     */
    private static class Organization implements Binarylizable {

        /** Organization ID. */
        @QuerySqlField(index = true)
        private int id;

        /** Organization name. */
        @QuerySqlField(index = true)
        private String name;

        /**
         * Constructs empty organization.
         */
        public Organization() {
            // No-op.
        }

        /**
         * Constructs organization with given ID.
         *
         * @param id Organization ID.
         * @param name Organization name.
         */
        public Organization(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return Organization id.
         */
        public int getId() {
            return id;
        }

        /**
         * @param id Organization id.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         * @return Organization name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Organization name.
         */
        public void setName(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("id", id);
            writer.writeString("name", name);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            id = reader.readInt("id");
            name = reader.readString("name");
        }
    }
}