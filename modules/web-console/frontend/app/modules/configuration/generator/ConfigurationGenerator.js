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

import DFLT_DIALECTS from 'app/data/dialects.json';

import { EmptyBean, Bean, MethodBean } from './Beans';

export default ['JavaTypes', 'igniteClusterDefaults', 'igniteCacheDefaults', 'igniteIgfsDefaults', (JavaTypes, clusterDflts, cacheDflts, igfsDflts) => {
    class ConfigurationGenerator {
        static igniteConfigurationBean(cluster) {
            return new Bean('org.apache.ignite.configuration.IgniteConfiguration', 'cfg', cluster, clusterDflts);
        }

        static igfsConfigurationBean(igfs) {
            return new Bean('org.apache.ignite.configuration.FileSystemConfiguration', 'igfs', igfs, igfsDflts);
        }

        static cacheConfigurationBean(cache) {
            return new Bean('org.apache.ignite.configuration.CacheConfiguration', 'cache', cache, cacheDflts);
        }

        static domainConfigurationBean(domain) {
            return new Bean('org.apache.ignite.cache.store.jdbc.JdbcType', 'type', domain, cacheDflts);
        }

        static discoveryConfigurationBean(discovery) {
            return new Bean('org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi', 'discovery', discovery, clusterDflts.discovery);
        }

        /**
         * Function to generate ignite configuration.
         *
         * @param {Object} cluster Cluster to process.
         * @param {Boolean} server
         * @return {Bean} Generated ignite configuration.
         */
        static igniteConfiguration(cluster, server) {
            const cfg = this.igniteConfigurationBean(cluster);

            this.clusterGeneral(cluster, cfg);
            this.clusterAtomics(cluster.atomicConfiguration, cfg);
            this.clusterBinary(cluster.binaryConfiguration, cfg);
            this.clusterCacheKeyConfiguration(cluster.cacheKeyConfiguration, cfg);
            this.clusterCollision(cluster.collision, cfg);
            this.clusterCommunication(cluster, cfg);
            this.clusterConnector(cluster.connector, cfg);
            this.clusterDeployment(cluster, cfg);
            this.clusterEvents(cluster, cfg);
            this.clusterFailover(cluster, cfg);
            this.clusterLogger(cluster.logger, cfg);
            this.clusterODBC(cluster.odbc, cfg);
            this.clusterMarshaller(cluster, cfg);
            this.clusterMetrics(cluster, cfg);
            this.clusterSwap(cluster, cfg);
            this.clusterTime(cluster, cfg);
            this.clusterPools(cluster, cfg);
            this.clusterTransactions(cluster.transactionConfiguration, cfg);
            this.clusterCaches(cluster, cluster.caches, cluster.igfss, server, cfg);
            this.clusterSsl(cluster, cfg);

            // TODO IGNITE-2052
            // if (server)
            //     $generatorSpring.igfss(cluster.igfss, res);

            this.clusterUserAttributes(cluster, cfg);

            return cfg;
        }

        static dialectClsName(dialect) {
            return DFLT_DIALECTS[dialect] || 'Unknown database: ' + (dialect || 'Choose JDBC dialect');
        }

        static dataSourceBean(id, dialect) {
            let dsBean;

            switch (dialect) {
                case 'Generic':
                    dsBean = new Bean('com.mchange.v2.c3p0.ComboPooledDataSource', id, {})
                        .property('Property', 'jdbcUrl', `${id}.jdbc.url`);

                    break;
                case 'Oracle':
                    dsBean = new Bean('oracle.jdbc.pool.OracleDataSource', id, {})
                        .property('Property', 'URL', `${id}.jdbc.url`);

                    break;
                case 'DB2':
                    dsBean = new Bean('com.ibm.db2.jcc.DB2DataSource', id, {})
                        .property('Property', 'serverName', `${id}.jdbc.server_name`)
                        .property('Property', 'portNumber', `${id}.jdbc.port_number`)
                        .property('Property', 'databaseName', `${id}.jdbc.database_name`)
                        .property('Property', 'driverType', `${id}.jdbc.driver_type`);

                    break;
                case 'SQLServer':
                    dsBean = new Bean('com.microsoft.sqlserver.jdbc.SQLServerDataSource', id, {})
                        .property('Property', 'URL', `${id}.jdbc.url`);

                    break;
                case 'MySQL':
                    dsBean = new Bean('com.mysql.jdbc.jdbc2.optional.MysqlDataSource', id, {})
                        .property('Property', 'URL', `${id}.jdbc.url`);

                    break;
                case 'PostgreSQL':
                    dsBean = new Bean('org.postgresql.ds.PGPoolingDataSource', id, {})
                        .property('Property', 'url', `${id}.jdbc.url`);

                    break;
                case 'H2':
                    dsBean = new Bean('org.h2.jdbcx.JdbcDataSource', id, {})
                        .property('Property', 'URL', `${id}.jdbc.url`);

                    break;
                default:
            }

            if (dsBean) {
                dsBean.property('Property', 'user', `${id}.jdbc.username`)
                    .property('Property', 'password', `${id}.jdbc.password`);
            }

            return dsBean;
        }

        // Generate general section.
        static clusterGeneral(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            cfg.stringProperty('name', 'gridName')
                .stringProperty('localHost');

            if (_.isNil(cluster.discovery))
                return cfg;

            const discovery = new Bean('org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi', 'discovery',
                cluster.discovery, clusterDflts.discovery);

            let ipFinder;

            switch (discovery.valueOf('kind')) {
                case 'Vm':
                    ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder',
                        'ipFinder', cluster.discovery.Vm, clusterDflts.discovery.Vm);

                    ipFinder.collectionProperty('addrs', 'addresses', cluster.discovery.Vm.addresses);

                    break;
                case 'Multicast':
                    ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder',
                        'ipFinder', cluster.discovery.Multicast, clusterDflts.discovery.Multicast);

                    ipFinder.stringProperty('multicastGroup')
                        .intProperty('multicastPort')
                        .intProperty('responseWaitTime')
                        .intProperty('addressRequestAttempts')
                        .stringProperty('localAddress')
                        .collectionProperty('addrs', 'addresses', cluster.discovery.Multicast.addresses);

                    break;
                case 'S3':
                    ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder',
                        'ipFinder', cluster.discovery.S3, clusterDflts.discovery.S3);

                    ipFinder.stringProperty('bucketName');

                    break;
                case 'Cloud':
                    ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.cloud.TcpDiscoveryCloudIpFinder',
                        'ipFinder', cluster.discovery.Cloud, clusterDflts.discovery.Cloud);

                    ipFinder.stringProperty('credential')
                        .pathProperty('credentialPath')
                        .stringProperty('identity')
                        .stringProperty('provider')
                        .collectionProperty('regions', 'regions', cluster.discovery.Cloud.regions)
                        .collectionProperty('zones', 'zones', cluster.discovery.Cloud.zones);

                    break;
                case 'GoogleStorage':
                    ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinder',
                        'ipFinder', cluster.discovery.GoogleStorage, clusterDflts.discovery.GoogleStorage);

                    ipFinder.stringProperty('projectName')
                        .stringProperty('bucketName')
                        .pathProperty('serviceAccountP12FilePath')
                        .stringProperty('serviceAccountId');

                    break;
                case 'Jdbc':
                    ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder',
                        'ipFinder', cluster.discovery.Jdbc, clusterDflts.discovery.Jdbc);

                    ipFinder.intProperty('initSchema');

                    if (ipFinder.includes('dataSourceBean', 'dialect')) {
                        const id = ipFinder.valueOf('dataSourceBean');

                        ipFinder.dataSource(id, 'dataSource', this.dataSourceBean(id, ipFinder.valueOf('dialect')));
                    }

                    break;
                case 'SharedFs':
                    ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder',
                        'ipFinder', cluster.discovery.SharedFs, clusterDflts.discovery.SharedFs);

                    ipFinder.pathProperty('path');

                    break;
                case 'ZooKeeper':
                    const src = cluster.discovery.ZooKeeper;
                    const dflt = clusterDflts.discovery.ZooKeeper;

                    ipFinder = new Bean('org.apache.ignite.spi.discovery.tcp.ipfinder.zk.TcpDiscoveryZookeeperIpFinder',
                        'ipFinder', src, dflt);

                    ipFinder.emptyBeanProperty('curator')
                        .stringProperty('zkConnectionString');

                    if (src && src.retryPolicy && src.retryPolicy.kind) {
                        const policy = src.retryPolicy;

                        let retryPolicyBean;

                        switch (policy.kind) {
                            case 'ExponentialBackoff':
                                retryPolicyBean = new Bean('org.apache.curator.retry.ExponentialBackoffRetry', null,
                                    policy.ExponentialBackoff, dflt.ExponentialBackoff)
                                    .intConstructorArgument('baseSleepTimeMs')
                                    .intConstructorArgument('maxRetries')
                                    .intConstructorArgument('maxSleepMs');

                                break;
                            case 'BoundedExponentialBackoff':
                                retryPolicyBean = new Bean('org.apache.curator.retry.BoundedExponentialBackoffRetry',
                                    null, policy.BoundedExponentialBackoffRetry, dflt.BoundedExponentialBackoffRetry)
                                    .intConstructorArgument('baseSleepTimeMs')
                                    .intConstructorArgument('maxSleepTimeMs')
                                    .intConstructorArgument('maxRetries');

                                break;
                            case 'UntilElapsed':
                                retryPolicyBean = new Bean('org.apache.curator.retry.RetryUntilElapsed', null,
                                    policy.UntilElapsed, dflt.UntilElapsed)
                                    .intConstructorArgument('maxElapsedTimeMs')
                                    .intConstructorArgument('sleepMsBetweenRetries');

                                break;

                            case 'NTimes':
                                retryPolicyBean = new Bean('org.apache.curator.retry.RetryNTimes', null,
                                    policy.NTimes, dflt.NTimes)
                                    .intConstructorArgument('n')
                                    .intConstructorArgument('sleepMsBetweenRetries');

                                break;
                            case 'OneTime':
                                retryPolicyBean = new Bean('org.apache.curator.retry.RetryOneTime', null,
                                    policy.OneTime, dflt.OneTime)
                                    .intConstructorArgument('sleepMsBetweenRetry');

                                break;
                            case 'Forever':
                                retryPolicyBean = new Bean('org.apache.curator.retry.RetryForever', null,
                                    policy.Forever, dflt.Forever)
                                    .intConstructorArgument('retryIntervalMs');

                                break;
                            case 'Custom':
                                if (_.nonEmpty(policy.Custom.className))
                                    retryPolicyBean = new EmptyBean(policy.Custom.className);

                                break;
                            default:
                        }

                        if (retryPolicyBean)
                            ipFinder.beanProperty('retryPolicy', retryPolicyBean);
                    }

                    ipFinder.pathProperty('basePath', '/services')
                        .stringProperty('serviceName')
                        .stringProperty('allowDuplicateRegistrations');

                    break;
                default:
            }

            if (ipFinder)
                discovery.beanProperty('ipFinder', ipFinder);

            this.clusterDiscovery(cluster.discovery, cfg, discovery);

            return cfg;
        }

        static clusterCaches(cluster, caches, igfss, serve, cfg = this.igniteConfigurationBean(cluster)) {
            const ccfgs = _.map(caches, (cache) => this.cacheConfiguration(cache));

            cfg.varArgProperty('ccfgs', 'cacheConfiguration', ccfgs, 'org.apache.ignite.configuration.CacheConfiguration');

            return cfg;
        }

        // Generate atomics group.
        static clusterAtomics(atomics, cfg = this.igniteConfigurationBean()) {
            const acfg = new Bean('org.apache.ignite.configuration.AtomicConfiguration', 'atomicCfg',
                atomics, clusterDflts.atomics);

            acfg.enumProperty('cacheMode')
                .intProperty('atomicSequenceReserveSize');

            if (acfg.valueOf('cacheMode') === 'PARTITIONED')
                acfg.intProperty('backups');

            if (acfg.isEmpty())
                return cfg;

            cfg.beanProperty('atomicConfiguration', acfg);

            return cfg;
        }

        // Generate binary group.
        static clusterBinary(binary, cfg = this.igniteConfigurationBean()) {
            const binaryCfg = new Bean('org.apache.ignite.configuration.BinaryConfiguration', 'binaryCfg',
                binary, clusterDflts.binary);

            binaryCfg.emptyBeanProperty('idMapper')
                .emptyBeanProperty('nameMapper')
                .emptyBeanProperty('serializer');

            const typeCfgs = [];

            _.forEach(binary.typeConfigurations, (type) => {
                const typeCfg = new MethodBean('org.apache.ignite.binary.BinaryTypeConfiguration',
                    JavaTypes.toJavaName('binaryType', type.typeName), type, clusterDflts.binary.typeConfigurations);

                typeCfg.stringProperty('typeName')
                    .emptyBeanProperty('idMapper')
                    .emptyBeanProperty('nameMapper')
                    .emptyBeanProperty('serializer')
                    .intProperty('enum');

                if (typeCfg.nonEmpty())
                    typeCfgs.push(typeCfg);
            });

            binaryCfg.collectionProperty('types', 'typeConfigurations', typeCfgs, 'java.util.Collection',
                'org.apache.ignite.binary.BinaryTypeConfiguration');

            binaryCfg.boolProperty('compactFooter');

            if (binaryCfg.isEmpty())
                return cfg;

            cfg.beanProperty('binaryConfiguration', binaryCfg);

            return cfg;
        }

        // Generate cache key configurations.
        static clusterCacheKeyConfiguration(keyCfgs, cfg = this.igniteConfigurationBean()) {
            const items = _.reduce(keyCfgs, (acc, keyCfg) => {
                if (keyCfg.typeName && keyCfg.affinityKeyFieldName) {
                    acc.push(new Bean('org.apache.ignite.cache.CacheKeyConfiguration', null, keyCfg)
                        .stringConstructorArgument('typeName')
                        .stringConstructorArgument('affinityKeyFieldName'));
                }

                return acc;
            }, []);

            if (_.isEmpty(items))
                return cfg;

            cfg.arrayProperty('cacheKeyConfiguration', 'keyConfigurations', items,
                'org.apache.ignite.cache.CacheKeyConfiguration');

            return cfg;
        }

        // Generate collision group.
        static clusterCollision(collision, cfg = this.igniteConfigurationBean()) {
            let colSpi;

            switch (collision.kind) {
                case 'JobStealing':
                    colSpi = new Bean('org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi',
                        'colSpi', collision.JobStealing, clusterDflts.collision.JobStealing);

                    colSpi.intProperty('activeJobsThreshold')
                        .intProperty('waitJobsThreshold')
                        .intProperty('messageExpireTime')
                        .intProperty('maximumStealingAttempts')
                        .boolProperty('stealingEnabled')
                        .emptyBeanProperty('externalCollisionListener')
                        .mapProperty('stealingAttrs', 'stealingAttributes');

                    break;
                case 'FifoQueue':
                    colSpi = new Bean('org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi',
                        'colSpi', collision.FifoQueue, clusterDflts.collision.FifoQueue);

                    colSpi.intProperty('parallelJobsNumber')
                        .intProperty('waitingJobsNumber');

                    break;
                case 'PriorityQueue':
                    colSpi = new Bean('org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi',
                        'colSpi', collision.PriorityQueue, clusterDflts.collision.PriorityQueue);

                    colSpi.intProperty('parallelJobsNumber')
                        .intProperty('waitingJobsNumber')
                        .intProperty('priorityAttributeKey')
                        .intProperty('jobPriorityAttributeKey')
                        .intProperty('defaultPriority')
                        .intProperty('starvationIncrement')
                        .boolProperty('starvationPreventionEnabled');

                    break;
                case 'Custom':
                    colSpi = new Bean(collision.Custom.class,
                        'colSpi', collision.PriorityQueue, clusterDflts.collision.PriorityQueue);

                    break;
                default:
                    return cfg;
            }

            if (colSpi.isEmpty())
                return cfg;

            cfg.beanProperty('collisionSpi', colSpi);

            return cfg;
        }

        // Generate communication group.
        static clusterCommunication(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            const commSpi = new Bean('org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi', 'communicationSpi',
                cluster.communication, clusterDflts.communication);

            commSpi.emptyBeanProperty('listener')
                .stringProperty('localAddress')
                .intProperty('localPort')
                .intProperty('localPortRange')
                .intProperty('sharedMemoryPort')
                .intProperty('directBuffer')
                .intProperty('directSendBuffer')
                .intProperty('idleConnectionTimeout')
                .intProperty('connectTimeout')
                .intProperty('maxConnectTimeout')
                .intProperty('reconnectCount')
                .intProperty('socketSendBuffer')
                .intProperty('socketReceiveBuffer')
                .intProperty('messageQueueLimit')
                .intProperty('slowClientQueueLimit')
                .intProperty('tcpNoDelay')
                .intProperty('ackSendThreshold')
                .intProperty('unacknowledgedMessagesBufferSize')
                .intProperty('socketWriteTimeout')
                .intProperty('selectorsCount')
                .emptyBeanProperty('addressResolver');

            if (commSpi.nonEmpty())
                cfg.beanProperty('communicationSpi', commSpi);

            cfg.intProperty('networkTimeout')
                .intProperty('networkSendRetryDelay')
                .intProperty('networkSendRetryCount')
                .intProperty('discoveryStartupDelay');

            return cfg;
        }

        // Generate REST access configuration.
        static clusterConnector(connector, cfg = this.igniteConfigurationBean()) {
            const connCfg = new Bean('org.apache.ignite.configuration.ConnectorConfiguration',
                'connectorConfiguration', connector, clusterDflts.connector);

            if (connCfg.valueOf('enabled')) {
                connCfg.pathProperty('jettyPath')
                    .stringProperty('host')
                    .intProperty('port')
                    .intProperty('portRange')
                    .intProperty('idleTimeout')
                    .intProperty('idleQueryCursorTimeout')
                    .intProperty('idleQueryCursorCheckFrequency')
                    .intProperty('receiveBufferSize')
                    .intProperty('sendBufferSize')
                    .intProperty('sendQueueLimit')
                    .intProperty('directBuffer')
                    .intProperty('noDelay')
                    .intProperty('selectorCount')
                    .intProperty('threadPoolSize')
                    .emptyBeanProperty('messageInterceptor')
                    .stringProperty('secretKey');

                if (connCfg.valueOf('sslEnabled')) {
                    connCfg.intProperty('sslClientAuth')
                        .emptyBeanProperty('sslFactory');
                }

                if (connCfg.nonEmpty())
                    cfg.beanProperty('connectorConfiguration', connCfg);
            }

            return cfg;
        }

        // Generate deployment group.
        static clusterDeployment(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            cfg.enumProperty('deploymentMode')
                .boolProperty('peerClassLoadingEnabled');

            if (cfg.valueOf('peerClassLoadingEnabled')) {
                cfg.intProperty('peerClassLoadingMissedResourcesCacheSize')
                    .intProperty('peerClassLoadingThreadPoolSize')
                    .varArgProperty('p2pLocClsPathExcl', 'peerClassLoadingLocalClassPathExclude',
                       cluster.peerClassLoadingLocalClassPathExclude);
            }

            return cfg;
        }

        // Generate discovery group.
        static clusterDiscovery(discovery, cfg = this.igniteConfigurationBean(), discoSpi = this.discoveryConfigurationBean(discovery)) {
            discoSpi.stringProperty('localAddress')
                .intProperty('localPort')
                .intProperty('localPortRange')
                .emptyBeanProperty('addressResolver')
                .intProperty('socketTimeout')
                .intProperty('ackTimeout')
                .intProperty('maxAckTimeout')
                .intProperty('networkTimeout')
                .intProperty('joinTimeout')
                .intProperty('threadPriority')
                .intProperty('heartbeatFrequency')
                .intProperty('maxMissedHeartbeats')
                .intProperty('maxMissedClientHeartbeats')
                .intProperty('topHistorySize')
                .emptyBeanProperty('listener')
                .emptyBeanProperty('dataExchange')
                .emptyBeanProperty('metricsProvider')
                .intProperty('reconnectCount')
                .intProperty('statisticsPrintFrequency')
                .intProperty('ipFinderCleanFrequency')
                .emptyBeanProperty('authenticator')
                .intProperty('forceServerMode')
                .intProperty('clientReconnectDisabled');

            if (discoSpi.nonEmpty())
                cfg.beanProperty('discoverySpi', discoSpi);

            return discoSpi;
        }

        // Generate events group.
        static clusterEvents(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            if (_.nonEmpty(cluster.includeEventTypes))
                cfg.eventTypes('events', 'includeEventTypes', cluster.includeEventTypes);

            return cfg;
        }

        // Generate failover group.
        static clusterFailover(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            const spis = [];

            _.forEach(cluster.failoverSpi, (spi) => {
                let failoverSpi;

                switch (spi.kind) {
                    case 'JobStealing':
                        failoverSpi = new Bean('org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi',
                            'failoverSpi', spi.JobStealing, clusterDflts.failoverSpi.JobStealing);

                        failoverSpi.intProperty('maximumFailoverAttempts');

                        break;
                    case 'Never':
                        failoverSpi = new Bean('org.apache.ignite.spi.failover.never.NeverFailoverSpi',
                            'failoverSpi', spi.Never);

                        break;
                    case 'Always':
                        failoverSpi = new Bean('org.apache.ignite.spi.failover.always.AlwaysFailoverSpi',
                            'failoverSpi', spi.Always, clusterDflts.failoverSpi.Always);

                        failoverSpi.intProperty('maximumFailoverAttempts');

                        break;
                    case 'Custom':
                        if (spi.Custom.class)
                            failoverSpi = new EmptyBean(spi.Custom.class);

                        break;
                    default:
                }

                if (failoverSpi)
                    spis.push(failoverSpi);
            });

            if (spis.length)
                cfg.arrayProperty('failoverSpi', 'failoverSpi', spis, 'org.apache.ignite.spi.failover.FailoverSpi');

            return cfg;
        }

        // Generate logger group.
        static clusterLogger(logger, cfg = this.igniteConfigurationBean()) {
            if (_.isNil(logger))
                return cfg;

            let loggerBean;

            switch (logger.kind) {
                case 'Log4j':
                    if (logger.Log4j && (logger.Log4j.mode === 'Default' || logger.Log4j.mode === 'Path' && _.nonEmpty(logger.Log4j.path))) {
                        loggerBean = new Bean('org.apache.ignite.logger.log4j.Log4JLogger',
                            'logger', logger.Log4j, clusterDflts.logger.Log4j);

                        if (loggerBean.valueOf('mode') === 'Path')
                            loggerBean.pathConstructorArgument('path');

                        loggerBean.enumProperty('level');
                    }

                    break;
                case 'Log4j2':
                    if (logger.Log4j2 && _.nonEmpty(logger.Log4j2.path)) {
                        loggerBean = new Bean('org.apache.ignite.logger.log4j2.Log4J2Logger',
                            'logger', logger.Log4j2, clusterDflts.logger.Log4j2);

                        loggerBean.pathConstructorArgument('path')
                            .enumProperty('level');
                    }

                    break;
                case 'Null':
                    loggerBean = new EmptyBean('org.apache.ignite.logger.NullLogger');

                    break;
                case 'Java':
                    loggerBean = new EmptyBean('org.apache.ignite.logger.java.JavaLogger');

                    break;
                case 'JCL':
                    loggerBean = new EmptyBean('org.apache.ignite.logger.jcl.JclLogger');

                    break;
                case 'SLF4J':
                    loggerBean = new EmptyBean('org.apache.ignite.logger.slf4j.Slf4jLogger');

                    break;
                case 'Custom':
                    if (logger.Custom && _.nonEmpty(logger.Custom.class))
                        loggerBean = new EmptyBean(logger.Custom.class);

                    break;
                default:
            }

            if (loggerBean)
                cfg.beanProperty('gridLogger', loggerBean);

            return cfg;
        }

        // Generate marshaller group.
        static clusterMarshaller(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            const marshaller = cluster.marshaller;

            if (marshaller && marshaller.kind) {
                let bean;

                switch (marshaller.kind) {
                    case 'OptimizedMarshaller':
                        bean = new Bean('org.apache.ignite.marshaller.optimized.OptimizedMarshaller', 'marshaller',
                            marshaller[marshaller.kind]);

                        bean.intProperty('poolSize')
                            .intProperty('requireSerializable');

                        break;

                    case 'JdkMarshaller':
                        bean = new Bean('org.apache.ignite.marshaller.jdk.JdkMarshaller', 'marshaller',
                            marshaller[marshaller.kind]);

                        break;

                    default:
                }

                if (bean)
                    cfg.beanProperty('marshaller', bean);
            }

            cfg.intProperty('marshalLocalJobs')
                .intProperty('marshallerCacheKeepAliveTime')
                .intProperty('marshallerCacheThreadPoolSize', 'marshallerCachePoolSize');

            return cfg;
        }

        // Generate metrics group.
        static clusterMetrics(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            cfg.intProperty('metricsExpireTime')
                .intProperty('metricsHistorySize')
                .intProperty('metricsLogFrequency')
                .intProperty('metricsUpdateFrequency');

            return cfg;
        }

        // Generate ODBC group.
        static clusterODBC(odbc, cfg = this.igniteConfigurationBean()) {
            if (_.get(odbc, 'odbcEnabled') !== true)
                return cfg;

            const bean = new Bean('org.apache.ignite.configuration.OdbcConfiguration', 'odbcConfiguration',
                odbc, clusterDflts.odbcConfiguration);

            bean.stringProperty('endpointAddress')
                .intProperty('maxOpenCursors');

            cfg.beanProperty('odbcConfiguration', bean);

            return cfg;
        }

        // Java code generator for cluster's SSL configuration.
        static clusterSsl(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            if (cluster.sslEnabled && _.nonNil(cluster.sslContextFactory)) {
                const bean = new Bean('org.apache.ignite.ssl.SslContextFactory', 'sslCtxFactory',
                    cluster.sslContextFactory);

                bean.intProperty('keyAlgorithm')
                    .pathProperty('keyStoreFilePath');

                if (_.nonEmpty(bean.valueOf('keyStoreFilePath')))
                    bean.propertyChar('keyStorePassword', 'ssl.key.storage.password');

                bean.intProperty('keyStoreType')
                    .intProperty('protocol');

                if (_.nonEmpty(cluster.sslContextFactory.trustManagers)) {
                    bean.arrayProperty('trustManagers', 'trustManagers',
                        _.map(cluster.sslContextFactory.trustManagers, (clsName) => new EmptyBean(clsName)),
                        'javax.net.ssl.TrustManager');
                }
                else {
                    bean.pathProperty('trustStoreFilePath');

                    if (_.nonEmpty(bean.valueOf('trustStoreFilePath')))
                        bean.propertyChar('trustStorePassword', 'ssl.trust.storage.password');

                    bean.intProperty('trustStoreType');
                }

                cfg.beanProperty('sslContextFactory', bean);
            }

            return cfg;
        }

        // Generate swap group.
        static clusterSwap(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            if (cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind === 'FileSwapSpaceSpi') {
                const bean = new Bean('org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi', 'swapSpaceSpi',
                    cluster.swapSpaceSpi.FileSwapSpaceSpi);

                bean.pathProperty('baseDirectory')
                    .intProperty('readStripesNumber')
                    .intProperty('maximumSparsity')
                    .intProperty('maxWriteQueueSize')
                    .intProperty('writeBufferSize');

                cfg.beanProperty('swapSpaceSpi', bean);
            }

            return cfg;
        }

        // Generate time group.
        static clusterTime(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            cfg.intProperty('clockSyncSamples')
                .intProperty('clockSyncFrequency')
                .intProperty('timeServerPortBase')
                .intProperty('timeServerPortRange');

            return cfg;
        }

        // Generate thread pools group.
        static clusterPools(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            cfg.intProperty('publicThreadPoolSize')
                .intProperty('systemThreadPoolSize')
                .intProperty('managementThreadPoolSize')
                .intProperty('igfsThreadPoolSize')
                .intProperty('rebalanceThreadPoolSize');

            return cfg;
        }

        // Generate transactions group.
        static clusterTransactions(transactionConfiguration, cfg = this.igniteConfigurationBean()) {
            const bean = new Bean('org.apache.ignite.configuration.TransactionConfiguration', 'transactionConfiguration',
                transactionConfiguration, clusterDflts.transactionConfiguration);

            bean.enumProperty('defaultTxConcurrency')
                .enumProperty('defaultTxIsolation')
                .intProperty('defaultTxTimeout')
                .intProperty('pessimisticTxLogLinger')
                .intProperty('pessimisticTxLogSize')
                .boolProperty('txSerializableEnabled')
                .emptyBeanProperty('txManagerFactory');

            if (bean.nonEmpty())
                cfg.beanProperty('transactionConfiguration', bean);

            return cfg;
        }

        // Generate user attributes group.
        static clusterUserAttributes(cluster, cfg = this.igniteConfigurationBean(cluster)) {
            cfg.mapProperty('attrs', 'attributes', 'userAttributes');

            return cfg;
        }

        // Generate domain model for general group.
        static domainModelGeneral(domain, cfg = this.domainConfigurationBean(domain)) {
            switch (cfg.valueOf('queryMetadata')) {
                case 'Annotations':
                    if (_.nonNil(domain.keyType) && _.nonNil(domain.valueType))
                        cfg.varArgProperty('indexedTypes', 'indexedTypes', [domain.keyType, domain.valueType], 'java.lang.Class');

                    break;
                case 'Configuration':
                    cfg.classProperty('keyType')
                        .classProperty('valueType');

                    break;
                default:
            }

            return cfg;
        }

        // Generate domain model for query group.
        static domainModelQuery(domain, cfg = this.domainConfigurationBean(domain)) {
            if (cfg.valueOf('queryMetadata') === 'Configuration') {
                const fields = _.map(cfg.valueOf('fields'),
                    (e) => ({name: e.name, className: JavaTypes.fullClassName(e.className)}));

                cfg.mapProperty('fields', fields, 'fields', true)
                    .mapProperty('aliases', 'aliases');

                const indexes = _.map(domain.indexes, (index) =>
                    new Bean('org.apache.ignite.cache.QueryIndex', 'index', index, cacheDflts.indexes)
                        .stringProperty('name')
                        .enumProperty('indexType')
                        .mapProperty('indFlds', 'fields', 'fields', true)
                );

                cfg.collectionProperty('indexes', 'indexes', indexes, 'java.util.Collection', 'org.apache.ignite.cache.QueryIndex');
            }

            return cfg;
        }


        // Generate domain model db fields.
        static _domainModelDatabaseFields(cfg, propName, domain) {
            const fields = _.map(domain[propName], (field) => {
                return new Bean('org.apache.ignite.cache.store.jdbc.JdbcTypeField', 'typeField', field, cacheDflts.typeField)
                    .constantConstructorArgument('databaseFieldType')
                    .stringConstructorArgument('databaseFieldName')
                    .classConstructorArgument('javaFieldType')
                    .stringConstructorArgument('javaFieldName');
            });

            cfg.varArgProperty(propName, propName, fields, 'org.apache.ignite.cache.store.jdbc.JdbcTypeField');

            return cfg;
        }

        // Generate domain model for store group.
        static domainStore(domain, cfg = this.domainConfigurationBean(domain)) {
            cfg.stringProperty('databaseSchema')
                .stringProperty('databaseTable');

            this._domainModelDatabaseFields(cfg, 'keyFields', domain);
            this._domainModelDatabaseFields(cfg, 'valueFields', domain);

            return cfg;
        }

        // static cacheDomains(domains, ccfg) {
        //     return ccfg;
        // }

        /**
         * Generate eviction policy object.
         * @param {Object} ccfg Parent configuration.
         * @param {String} name Property name.
         * @param {Object} src Source.
         * @param {Object} dflt Default.
         * @returns {Object} Parent configuration.
         * @private
         */
        static _evictionPolicy(ccfg, name, src, dflt) {
            let bean;

            switch (_.get(src, 'kind')) {
                case 'LRU':
                    bean = new Bean('org.apache.ignite.cache.eviction.lru.LruEvictionPolicy', 'evictionPlc',
                        src.LRU, dflt.LRU);

                    break;
                case 'FIFO':
                    bean = new Bean('org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy', 'evictionPlc',
                        src.FIFO, dflt.FIFO);

                    break;
                case 'SORTED':
                    bean = new Bean('org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy', 'evictionPlc',
                        src.SORTED, dflt.SORTED);

                    break;
                default:
                    return ccfg;
            }

            bean.intProperty('batchSize')
                .intProperty('maxMemorySize')
                .intProperty('maxSize');

            ccfg.beanProperty(name, bean);

            return ccfg;
        }

        // Generate cache general group.
        static cacheGeneral(cache, ccfg = this.cacheConfigurationBean(cache)) {
            ccfg.stringProperty('name')
                .enumProperty('cacheMode')
                .enumProperty('atomicityMode');

            if (ccfg.valueOf('cacheMode') === 'PARTITIONED' && ccfg.valueOf('backups')) {
                ccfg.intProperty('backups')
                    .intProperty('readFromBackup');
            }

            ccfg.intProperty('copyOnRead');

            if (ccfg.valueOf('cacheMode') === 'PARTITIONED' && ccfg.valueOf('atomicityMode') === 'TRANSACTIONAL')
                ccfg.intProperty('invalidate');

            return ccfg;
        }

        // Generate cache memory group.
        static cacheMemory(cache, ccfg = this.cacheConfigurationBean(cache)) {
            ccfg.enumProperty('memoryMode');

            if (ccfg.valueOf('memoryMode') !== 'OFFHEAP_VALUES')
                ccfg.intProperty('offHeapMaxMemory');

            this._evictionPolicy(ccfg, 'evictionPolicy', cache.evictionPolicy, cacheDflts.evictionPolicy);

            ccfg.intProperty('startSize')
                .boolProperty('swapEnabled');

            return ccfg;
        }

        // Generate cache queries & Indexing group.
        static cacheQuery(cache, domains, ccfg = this.cacheConfigurationBean(cache)) {
            const indexedTypes = _.reduce(domains, (acc, domain) => {
                if (domain.queryMetadata === 'Annotations')
                    acc.push(domain.keyType, domain.valueType);

                return acc;
            }, []);

            ccfg.stringProperty('sqlSchema')
                .intProperty('sqlOnheapRowCacheSize')
                .intProperty('longQueryWarningTimeout')
                .arrayProperty('indexedTypes', 'indexedTypes', indexedTypes, 'java.lang.Class')
                .arrayProperty('sqlFunctionClasses', 'sqlFunctionClasses', cache.sqlFunctionClasses, 'java.lang.Class')
                .intProperty('snapshotableIndex')
                .intProperty('sqlEscapeAll');

            return ccfg;
        }

        // Generate cache store group.
        static cacheStore(cache, domains, ccfg = this.cacheConfigurationBean(cache)) {
            const kind = _.get(cache, 'cacheStoreFactory.kind');

            if (kind && cache.cacheStoreFactory[kind]) {
                let bean = null;

                const storeFactory = cache.cacheStoreFactory[kind];

                switch (kind) {
                    case 'CacheJdbcPojoStoreFactory':
                        bean = new Bean('org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory', 'cacheStoreFactory',
                            storeFactory);

                        const id = bean.valueOf('dataSourceBean');

                        bean.dataSource(id, 'dataSourceBean', this.dataSourceBean(id, storeFactory.dialect))
                            .beanProperty('dialect', new EmptyBean(this.dialectClsName(storeFactory.dialect)));

                        const setType = (typeBean, propName) => {
                            if (JavaTypes.nonBuiltInClass(typeBean.valueOf(propName)))
                                typeBean.stringProperty(propName);
                            else
                                typeBean.classProperty(propName);
                        };

                        const types = _.reduce(domains, (acc, domain) => {
                            if (_.isNil(domain.databaseTable))
                                return acc;

                            const typeBean = new MethodBean('org.apache.ignite.cache.store.jdbc.JdbcType', 'type',
                                _.merge({}, domain, {cacheName: cache.name}))
                                .stringProperty('cacheName');

                            setType(typeBean, 'keyType');
                            setType(typeBean, 'valueType');

                            this.domainStore(domain, typeBean);

                            acc.push(typeBean);

                            return acc;
                        }, []);

                        bean.arrayProperty('types', 'types', types, 'org.apache.ignite.cache.store.jdbc.JdbcType');

                        break;
                    case 'CacheJdbcBlobStoreFactory':
                        bean = new Bean('org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStoreFactory', 'cacheStoreFactory',
                            storeFactory);

                        if (bean.valueOf('connectVia') === 'DataSource')
                            bean.dataSource(bean.valueOf('dataSourceBean'), 'dataSourceBean', this.dialectClsName(storeFactory.dialect));
                        else {
                            ccfg.stringProperty('connectionUrl')
                                .stringProperty('user')
                                .property('Property', 'password', `ds.${storeFactory.user}.password`);
                        }

                        bean.boolProperty('initSchema')
                            .stringProperty('createTableQuery')
                            .stringProperty('loadQuery')
                            .stringProperty('insertQuery')
                            .stringProperty('updateQuery')
                            .stringProperty('deleteQuery');

                        break;
                    case 'CacheHibernateBlobStoreFactory':
                        bean = new Bean('org.apache.ignite.cache.store.hibernate.CacheHibernateBlobStoreFactory',
                            'cacheStoreFactory', storeFactory);

                        bean.propsProperty('props', 'hibernateProperties');

                        break;
                    default:
                }

                if (bean)
                    ccfg.beanProperty('cacheStoreFactory', bean);
            }

            ccfg.boolProperty('storeKeepBinary')
                .boolProperty('loadPreviousValue')
                .boolProperty('readThrough')
                .boolProperty('writeThrough');

            if (ccfg.valueOf('writeBehindEnabled')) {
                ccfg.boolProperty('writeBehindEnabled')
                    .intProperty('writeBehindBatchSize')
                    .intProperty('writeBehindFlushSize')
                    .intProperty('writeBehindFlushFrequency')
                    .intProperty('writeBehindFlushThreadCount');
            }

            return ccfg;
        }

        // Generate cache concurrency control group.
        static cacheConcurrency(cache, ccfg = this.cacheConfigurationBean(cache)) {
            ccfg.intProperty('maxConcurrentAsyncOperations')
                .intProperty('defaultLockTimeout')
                .enumProperty('atomicWriteOrderMode')
                .enumProperty('writeSynchronizationMode');

            return ccfg;
        }

        // Generate cache node filter group.
        static cacheNodeFilter(cache, igfss, ccfg = this.cacheConfigurationBean(cache)) {
            const kind = _.get(cache, 'nodeFilter.kind');

            if (kind && cache.nodeFilter[kind]) {
                let bean = null;

                switch (kind) {
                    case 'IGFS':
                        const foundIgfs = _.find(igfss, (igfs) => igfs._id === cache.nodeFilter.IGFS.igfs);

                        if (foundIgfs) {
                            bean = new Bean('org.apache.ignite.internal.processors.igfs.IgfsNodePredicate', 'nodeFilter', foundIgfs)
                                .stringConstructorArgument('name');
                        }

                        break;
                    case 'Custom':
                        bean = new Bean(cache.nodeFilter.Custom.className, 'nodeFilter');

                        break;
                    default:
                        return ccfg;
                }

                if (bean)
                    ccfg.beanProperty('nodeFilter', bean);
            }

            return ccfg;
        }

        // Generate cache rebalance group.
        static cacheRebalance(cache, ccfg = this.cacheConfigurationBean(cache)) {
            if (ccfg.valueOf('cacheMode') !== 'LOCAL') {
                ccfg.enumProperty('rebalanceMode')
                    .intProperty('rebalanceThreadPoolSize')
                    .intProperty('rebalanceBatchSize')
                    .intProperty('rebalanceBatchesPrefetchCount')
                    .intProperty('rebalanceOrder')
                    .intProperty('rebalanceDelay')
                    .intProperty('rebalanceTimeout')
                    .intProperty('rebalanceThrottle');
            }

            if (ccfg.includes('igfsAffinnityGroupSize')) {
                const bean = new Bean('org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper', 'affinityMapper', cache)
                    .intConstructorArgument('igfsAffinnityGroupSize');

                ccfg.beanProperty('affinityMapper', bean);
            }

            return ccfg;
        }

        // Generate server near cache group.
        static cacheNearServer(cache, ccfg = this.cacheConfigurationBean(cache)) {
            if (ccfg.valueOf('cacheMode') === 'PARTITIONED' && _.get(cache, 'nearConfiguration.enabled')) {
                const bean = new Bean('org.apache.ignite.configuration.NearCacheConfiguration', 'nearConfiguration',
                    cache.nearConfiguration, cacheDflts.nearConfiguration);

                bean.intProperty('nearStartSize');

                this._evictionPolicy(bean, 'nearEvictionPolicy',
                    bean.valueOf('nearEvictionPolicy'), cacheDflts.evictionPolicy);

                ccfg.beanProperty('nearConfiguration', bean);
            }

            return ccfg;
        }

        // Generate client near cache group.
        static cacheNearClient(cache, ccfg = this.cacheConfigurationBean(cache)) {
            if (ccfg.valueOf('cacheMode') === 'PARTITIONED' && _.get(cache, 'clientNearConfiguration.enabled')) {
                const bean = new Bean('org.apache.ignite.configuration.NearCacheConfiguration',
                    JavaTypes.toJavaName('nearConfiguration', ccfg.valueOf('name')),
                    cache.clientNearConfiguration, cacheDflts.clientNearConfiguration);

                bean.intProperty('nearStartSize');

                this._evictionPolicy(bean, 'nearEvictionPolicy',
                    bean.valueOf('nearEvictionPolicy'), cacheDflts.evictionPolicy);

                return bean;
            }

            return ccfg;
        }

        // Generate cache statistics group.
        static cacheStatistics(cache, ccfg = this.cacheConfigurationBean(cache)) {
            ccfg.boolProperty('statisticsEnabled')
                .boolProperty('managementEnabled');

            return ccfg;
        }

        static cacheConfiguration(cache, ccfg = this.cacheConfigurationBean(cache)) {
            this.cacheGeneral(cache, ccfg);
            this.cacheMemory(cache, ccfg);
            this.cacheQuery(cache, cache.domains, ccfg);
            this.cacheStore(cache, cache.domains, ccfg);

            const igfs = _.get(cache, 'nodeFilter.IGFS.instance');
            this.cacheNodeFilter(cache, igfs ? [igfs] : [], ccfg);
            this.cacheConcurrency(cache, ccfg);
            this.cacheRebalance(cache, ccfg);
            this.cacheNearServer(cache, ccfg);
            this.cacheStatistics(cache, ccfg);
            // this.cacheDomains(cache.domains, cfg);

            return ccfg;
        }

        // Generate IGFS general group.
        static igfsGeneral(igfs, cfg = this.igfsConfigurationBean(igfs)) {
            if (_.isEmpty(igfs.name))
                return cfg;

            cfg.stringProperty('name')
                .property('java.lang.String', 'dataCacheName', igfs.name + '-data')
                .property('java.lang.String', 'metaCacheName', igfs.name + '-meta')
                .enumProperty('defaultMode');

            return cfg;
        }

        // Generate IGFS secondary file system group.
        static igfsSecondFS(igfs, cfg = this.igfsConfigurationBean(igfs)) {
            if (igfs.secondaryFileSystemEnabled) {
                const secondFs = igfs.secondaryFileSystem || {};

                const bean = new Bean('org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem',
                    'secondaryFileSystem', secondFs, igfsDflts.secondaryFileSystem);

                bean.stringProperty('userName', 'defaultUserName');

                const factoryBean = new Bean('org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory',
                    'fac', secondFs);

                factoryBean.stringProperty('uri')
                    .pathProperty('cfgPath', 'configPaths');

                bean.beanProperty('fileSystemFactory', factoryBean);

                cfg.beanProperty('secondaryFileSystem', bean);
            }

            return cfg;
        }

        // Generate IGFS IPC group.
        static igfsIPC(igfs, cfg = this.igfsConfigurationBean(igfs)) {
            if (igfs.ipcEndpointEnabled) {
                const bean = new Bean('org.apache.ignite.igfs.IgfsIpcEndpointConfiguration', 'ipcEndpointConfiguration',
                    igfs.ipcEndpointConfiguration, igfsDflts.ipcEndpointConfiguration);

                bean.enumProperty('type')
                    .stringProperty('host')
                    .intProperty('port')
                    .intProperty('memorySize')
                    .pathProperty('tokenDirectoryPath')
                    .intProperty('threadCount');

                cfg.beanProperty('ipcEndpointConfiguration', bean);
            }

            return cfg;
        }

        // Generate IGFS fragmentizer group.
        static igfsFragmentizer(igfs, cfg = this.igfsConfigurationBean(igfs)) {
            if (igfs.fragmentizerEnabled) {
                cfg.intProperty('fragmentizerConcurrentFiles')
                    .intProperty('fragmentizerThrottlingBlockLength')
                    .intProperty('fragmentizerThrottlingDelay');
            }
            else
                cfg.boolProperty('fragmentizerEnabled');

            return cfg;
        }

        // Generate IGFS Dual mode group.
        static igfsDualMode(igfs, cfg = this.igfsConfigurationBean(igfs)) {
            cfg.intProperty('dualModeMaxPendingPutsSize')
                .emptyBeanProperty('dualModePutExecutorService')
                .intProperty('dualModePutExecutorServiceShutdown');

            return cfg;
        }

        // Generate IGFS miscellaneous group.
        static igfsMisc(igfs, cfg = this.igfsConfigurationBean(igfs)) {
            cfg.intProperty('blockSize')
                .intProperty('streamBufferSize')
                .intProperty('maxSpaceSize')
                .intProperty('maximumTaskRangeLength')
                .intProperty('managementPort')
                .intProperty('perNodeBatchSize')
                .intProperty('perNodeParallelBatchCount')
                .intProperty('prefetchBlocks')
                .intProperty('sequentialReadsBeforePrefetch')
                .intProperty('trashPurgeTimeout')
                .intProperty('colocateMetadata')
                .intProperty('relaxedConsistency')
                .mapProperty('pathModes', 'pathModes');

            return cfg;
        }
    }

    return ConfigurationGenerator;
}];
