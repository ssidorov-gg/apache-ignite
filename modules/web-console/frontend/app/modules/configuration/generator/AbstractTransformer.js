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

import StringBuilder from './StringBuilder';

export default class AbstractTransformer {
    // Generate general section.
    static clusterGeneral(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterGeneral(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate atomics group.
    static clusterAtomics(atomics, sb = new StringBuilder()) {
        const cfg = this.generator.clusterAtomics(atomics);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate binary group.
    static clusterBinary(binary, sb = new StringBuilder()) {
        const cfg = this.generator.clusterBinary(binary);

        this._setProperties(sb, cfg);

        return sb;
    }
    // Generate cache key configurations.
    static clusterCacheKeyConfiguration(keyCfgs, sb = new StringBuilder()) {
        const cfg = this.generator.clusterCacheKeyConfiguration(keyCfgs);

        this._setProperties(sb, cfg);

        return sb;
    }

    static clusterCollision(collision, sb = new StringBuilder()) {
        const cfg = this.generator.clusterCollision(collision);

        this._setProperties(sb, cfg);

        return sb;
    }

    static clusterCommunication(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterCommunication(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    static clusterConnector(connector, sb = new StringBuilder()) {
        const cfg = this.generator.clusterConnector(connector);

        this._setProperties(sb, cfg);

        return sb;
    }

    static clusterDeployment(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterDeployment(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    static clusterDiscovery(disco, sb = new StringBuilder()) {
        const cfg = this.generator.clusterDiscovery(disco);

        this._setProperties(sb, cfg);

        return sb;
    }

    static clusterEvents(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterEvents(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    static clusterFailover(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterFailover(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate logger group.
    static clusterLogger(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterLogger(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate marshaller group.
    static clusterMarshaller(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterMarshaller(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate metrics group.
    static clusterMetrics(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterMetrics(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate ODBC group.
    static clusterODBC(odbc, sb = new StringBuilder()) {
        const cfg = this.generator.clusterODBC(odbc);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate ssl group.
    static clusterSsl(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterSsl(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate swap group.
    static clusterSwap(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterSwap(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate time group.
    static clusterTime(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterTime(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate thread pools group.
    static clusterPools(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterPools(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate transactions group.
    static clusterTransactions(transactionConfiguration, sb = new StringBuilder()) {
        const cfg = this.generator.clusterTransactions(transactionConfiguration);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate user attributes group.
    static clusterUserAttributes(cluster, sb = new StringBuilder()) {
        const cfg = this.generator.clusterUserAttributes(cluster);

        this._setProperties(sb, cfg);

        return sb;
    }

    // // Generate IGFS general group.
    static igfsGeneral(igfs, sb = new StringBuilder()) {
        const cfg = this.generator.igfsGeneral(igfs);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate IGFS secondary file system group.
    static igfsSecondFS(igfs, sb = new StringBuilder()) {
        const cfg = this.generator.igfsSecondFS(igfs);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate IGFS IPC group.
    static igfsIPC(igfs, sb = new StringBuilder()) {
        const cfg = this.generator.igfsIPC(igfs);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate IGFS fragmentizer group.
    static igfsFragmentizer(igfs, sb = new StringBuilder()) {
        const cfg = this.generator.igfsFragmentizer(igfs);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate IGFS Dual mode group.
    static igfsDualMode(igfs, sb = new StringBuilder()) {
        const cfg = this.generator.igfsDualMode(igfs);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate IGFS miscellaneous group.
    static igfsMisc(igfs, sb = new StringBuilder()) {
        const cfg = this.generator.igfsMisc(igfs);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate cache general group.
    static cacheGeneral(cache, sb = new StringBuilder()) {
        const cfg = this.generator.cacheGeneral(cache);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate cache memory group.
    static cacheMemory(cache, sb = new StringBuilder()) {
        const cfg = this.generator.cacheMemory(cache);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate cache queries & Indexing group.
    static cacheQuery(cache, domains, sb = new StringBuilder()) {
        const cfg = this.generator.cacheQuery(cache, domains);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate cache store group.
    static cacheStore(cache, domains, sb = new StringBuilder()) {
        const cfg = this.generator.cacheStore(cache, domains);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate cache concurrency control group.
    static cacheConcurrency(cache, sb = new StringBuilder()) {
        const cfg = this.generator.cacheConcurrency(cache);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate cache node filter group.
    static cacheNodeFilter(cache, igfss, sb = new StringBuilder()) {
        const cfg = this.generator.cacheNodeFilter(cache, igfss);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate cache rebalance group.
    static cacheRebalance(cache, sb = new StringBuilder()) {
        const cfg = this.generator.cacheRebalance(cache);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate server near cache group.
    static cacheServerNearCache(cache, sb = new StringBuilder()) {
        const cfg = this.generator.cacheServerNearCache(cache);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate cache statistics group.
    static cacheStatistics(cache, sb = new StringBuilder()) {
        const cfg = this.generator.cacheStatistics(cache);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate caches configs.
    static clusterCaches(cluster, caches, igfss, isSrvCfg, sb = new StringBuilder()) {
        const cfg = this.generator.clusterCaches(cluster, caches);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate domain model for general group.
    static domainModelGeneral(domain, sb = new StringBuilder()) {
        const cfg = this.generator.domainModelGeneral(domain);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate domain model for query group.
    static domainModelQuery(domain, sb = new StringBuilder()) {
        const cfg = this.generator.domainModelQuery(domain);

        this._setProperties(sb, cfg);

        return sb;
    }

    // Generate domain model for store group.
    static domainStore(domain, sb = new StringBuilder()) {
        const cfg = this.generator.domainStore(domain);

        this._setProperties(sb, cfg);

        return sb;
    }
}
