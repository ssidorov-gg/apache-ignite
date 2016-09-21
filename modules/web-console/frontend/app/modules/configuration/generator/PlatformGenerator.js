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

// import _ from 'lodash';
import { Bean } from './Beans';

export default ['JavaTypes', 'igniteClusterPlatformDefaults', (JavaTypes, clusterDflts) => {
    class PlatformGenerator {
        static igniteConfigurationBean(cluster) {
            return new Bean('Apache.Ignite.Core.IgniteConfiguration', 'cfg', cluster, clusterDflts);
        }

        /**
         * Function to generate ignite configuration.
         *
         * @param {Object} cluster Cluster to process.
         * @return {String} Generated ignite configuration.
         */
        static igniteConfiguration(cluster) {
            const cfg = this.igniteConfigurationBean(cluster);

            this.clusterAtomics(cluster.atomics, cfg);

            return cfg;
        }

        // Generate general section.
        static clusterGeneral(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            cfg.stringProperty('name', 'GridName')
                .stringProperty('localHost', 'Localhost');

            if (_.isNil(cluster.discovery))
                return cfg;

            const discovery = new Bean('Apache.Ignite.Core.Discovery.Tcp.TcpDiscoverySpi', 'discovery',
                cluster.discovery, clusterDflts.discovery);

            let ipFinder;

            switch (discovery.valueOf('kind')) {
                case 'Vm':
                    ipFinder = new Bean('Apache.Ignite.Core.Discovery.Tcp.Static.TcpDiscoveryStaticIpFinder',
                        'ipFinder', cluster.discovery.Vm, clusterDflts.discovery.Vm);

                    ipFinder.collectionProperty('addrs', 'addresses', cluster.discovery.Vm.addresses, 'ICollection');

                    break;
                case 'Multicast':
                    ipFinder = new Bean('Apache.Ignite.Core.Discovery.Tcp.Multicast.TcpDiscoveryMulticastIpFinder',
                        'ipFinder', cluster.discovery.Multicast, clusterDflts.discovery.Multicast);

                    ipFinder.stringProperty('MulticastGroup')
                        .intProperty('multicastPort', 'MulticastPort')
                        .intProperty('responseWaitTime', 'ResponseTimeout')
                        .intProperty('addressRequestAttempts', 'AddressRequestAttempts')
                        .stringProperty('localAddress', 'LocalAddress')
                        .collectionProperty('addrs', 'Endpoints', cluster.discovery.Multicast.addresses, 'ICollection');

                    break;
                default:
            }

            if (ipFinder)
                discovery.beanProperty('IpFinder', ipFinder);

            cfg.beanProperty('DiscoverySpi', discovery);


            return cfg;
        }

        static clusterAtomics(atomics, cfg = this.igniteConfigurationBean()) {
            const acfg = new Bean('Apache.Ignite.Core.DataStructures.Configuration.AtomicConfiguration', 'atomicCfg',
                atomics, clusterDflts.atomics);

            acfg.enumProperty('cacheMode', 'CacheMode')
                .intProperty('atomicSequenceReserveSize', 'AtomicSequenceReserveSize');

            if (acfg.valueOf('cacheMode') === 'PARTITIONED')
                acfg.intProperty('backups', 'Backups');

            if (acfg.isEmpty())
                return cfg;

            cfg.beanProperty('AtomicConfiguration', acfg);

            return cfg;
        }

        // Generate binary group.
        static clusterBinary(binary, cfg = this.igniteConfigurationBean()) {
            const binaryCfg = new Bean('Apache.Ignite.Core.Binary.BinaryConfiguration', 'binaryCfg',
                binary, clusterDflts.binary);

            binaryCfg.emptyBeanProperty('idMapper', 'DefaultIdMapper')
                .emptyBeanProperty('nameMapper', 'DefaultNameMapper')
                .emptyBeanProperty('serializer', 'DefaultSerializer');

            // const typeCfgs = [];
            //
            // _.forEach(binary.typeConfigurations, (type) => {
            //     const typeCfg = new MethodBean('Apache.Ignite.Core.Binary.BinaryTypeConfiguration',
            //         JavaTypes.toJavaName('binaryType', type.typeName), type, clusterDflts.binary.typeConfigurations);
            //
            //     typeCfg.stringProperty('typeName', 'TypeName')
            //         .emptyBeanProperty('idMapper', 'IdMapper')
            //         .emptyBeanProperty('nameMapper', 'NameMapper')
            //         .emptyBeanProperty('serializer', 'Serializer')
            //         .intProperty('enum', 'IsEnum');
            //
            //     if (typeCfg.nonEmpty())
            //         typeCfgs.push(typeCfg);
            // });
            //
            // binaryCfg.collectionProperty('types', 'TypeConfigurations', typeCfgs, 'ICollection',
            //     'Apache.Ignite.Core.Binary.BinaryTypeConfiguration');
            //
            // binaryCfg.boolProperty('compactFooter', 'CompactFooter');
            //
            // if (binaryCfg.isEmpty())
            //     return cfg;
            //
            // cfg.beanProperty('binaryConfiguration', binaryCfg);

            return cfg;
        }

        // Generate cache key configurations.
        static clusterCacheKeyConfiguration(keyCfgs, cfg = this.igniteConfigurationBean()) {
            return cfg;
        }

        // Generate collision group.
        static clusterCollision(collision, cfg = this.igniteConfigurationBean()) {
            return cfg;
        }

        // Generate communication group.
        static clusterCommunication(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            const commSpi = new Bean('Apache.Ignite.Core.Communication.Tcp.TcpCommunicationSpi', 'communicationSpi',
                cluster.communication, clusterDflts.communication);

            commSpi.emptyBeanProperty('listener')
                .stringProperty('localAddress')
                .intProperty('localPort', 'LocalPort')
                .intProperty('localPortRange', 'LocalPortRange')
                // .intProperty('sharedMemoryPort')
                .intProperty('directBuffer', 'DirectBuffer')
                .intProperty('directSendBuffer', 'DirectSendBuffer')
                .intProperty('idleConnectionTimeout', 'IdleConnectionTimeout')
                .intProperty('connectTimeout', 'ConnectTimeout')
                .intProperty('maxConnectTimeout', 'MaxConnectTimeout')
                .intProperty('reconnectCount', 'ReconnectCount')
                .intProperty('socketSendBuffer', 'SocketSendBufferSize')
                .intProperty('socketReceiveBuffer', 'SocketReceiveBufferSize')
                .intProperty('messageQueueLimit', 'MessageQueueLimit')
                .intProperty('slowClientQueueLimit', 'SlowClientQueueLimit')
                .intProperty('tcpNoDelay', 'TcpNoDelay')
                .intProperty('ackSendThreshold', 'AckSendThreshold')
                .intProperty('unacknowledgedMessagesBufferSize')
                // .intProperty('socketWriteTimeout')
                .intProperty('selectorsCount', 'SelectorsCount');
                // .emptyBeanProperty('addressResolver');

            if (commSpi.nonEmpty())
                cfg.beanProperty('CommunicationSpi', commSpi);

            cfg.intProperty('networkTimeout', 'NetworkTimeout')
                .intProperty('networkSendRetryDelay', 'NetworkSendRetryDelay')
                .intProperty('networkSendRetryCount', 'NetworkSendRetryCount');
                // .intProperty('discoveryStartupDelay');

            return cfg;
        }

        // Generate REST access configuration.
        static clusterConnector(connector, cfg = this.igniteConfigurationBean()) {
            return cfg;
        }

        // Generate deployment group.
        static clusterDeployment(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            return cfg;
        }

        // Generate discovery group.
        static clusterDiscovery(discovery, cfg = this.igniteConfigurationBean()) {
            return cfg;
        }

        // Generate events group.
        static clusterEvents(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            return cfg;
        }

        // Generate failover group.
        static clusterFailover(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            return cfg;
        }

        // Generate logger group.
        static clusterLogger(logger, cfg = this.igniteConfigurationBean()) {
            return cfg;
        }

        // Generate marshaller group.
        static clusterMarshaller(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            return cfg;
        }

        // Generate metrics group.
        static clusterMetrics(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            return cfg;
        }

        // Generate ODBC group.
        static clusterODBC(odbc, cfg = this.igniteConfigurationBean()) {
            return cfg;
        }

        // Java code generator for cluster's SSL configuration.
        static clusterSsl(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            return cfg;
        }

        // Generate swap group.
        static clusterSwap(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            return cfg;
        }

        // Generate time group.
        static clusterTime(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            return cfg;
        }

        // Generate thread pools group.
        static clusterPools(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            return cfg;
        }

        // Generate transactions group.
        static clusterTransactions(transactionConfiguration, cfg = this.igniteConfigurationBean()) {
            return cfg;
        }

        // Generate user attributes group.
        static clusterUserAttributes(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            return cfg;
        }

        static clusterCaches(cluster, caches, igfss, isSrvCfg, cfg = this.igniteConfigurationBean(cluster)) {
            // const cfg = this.clusterGeneral(cluster, cfg);
            //
            // if (_.nonEmpty(caches)) {
            //     const ccfgs = _.map(caches, (cache) => this.cacheConfiguration(cache));
            //
            //     cfg.collectionProperty('', '', ccfgs, );
            // }

            return this.clusterGeneral(cluster, cfg);
        }
    }

    return PlatformGenerator;
}];
