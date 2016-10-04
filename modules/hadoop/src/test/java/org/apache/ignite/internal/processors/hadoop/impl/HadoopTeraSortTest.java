package org.apache.ignite.internal.processors.hadoop.impl;

import com.google.common.base.Joiner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.IgniteHadoopFileSystemCounterWriter;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsUserContext;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.HadoopCommonUtils;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopPerformanceCounter;
import org.apache.ignite.internal.processors.hadoop.impl.examples.HadoopWordCount1;
import org.apache.ignite.internal.processors.hadoop.impl.examples.HadoopWordCount2;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraInputFormat;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraOutputFormat;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraSort;
import org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopFileSystemsUtils;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.createJobInfo;

/**
 *
 */
public class HadoopTeraSortTest extends HadoopAbstractSelfTest {
//    /** Input path. */
//    protected static final String PATH_INPUT = "/input";
//
//    /** Output path. */
//    protected static final String PATH_OUTPUT = "/output";
//
//    /** IGFS instance. */
//    protected IgfsEx igfs;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Configuration cfg = new Configuration();

        setupFileSystems(cfg);

        // Init cache by correct LocalFileSystem implementation
        FileSystem.getLocal(cfg);
    }

    @Override protected void setupFileSystems(Configuration cfg) {
        cfg.set("fs.defaultFS", "file:///");

        // TODO: Not sere if we really need that:
        HadoopFileSystemsUtils.setupFileSystems(cfg);
    }

//
//    /** {@inheritDoc} */
//    @Override protected void beforeTest() throws Exception {
//        super.beforeTest();
//        //igfs = (IgfsEx)startGrids(gridCount()).fileSystem(igfsName);
//    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected final boolean igfsEnabled() {
        return false;
    }

//    /** IGFS block size. */
//    protected static final int IGFS_BLOCK_SIZE = 512 * 1024;
//
//    /** Amount of blocks to prefetch. */
//    protected static final int PREFETCH_BLOCKS = 1;
//
//    /** Amount of sequential block reads before prefetch is triggered. */
//    protected static final int SEQ_READS_BEFORE_PREFETCH = 2;
//
//    /** Secondary file system URI. */
//    protected static final String SECONDARY_URI = "igfs://igfs-secondary:grid-secondary@127.0.0.1:11500/";
//
//    /** Secondary file system configuration path. */
//    protected static final String SECONDARY_CFG = "modules/core/src/test/config/hadoop/core-site-loopback-secondary.xml";

    /** The user to run Hadoop job on behalf of. */
    protected static final String USER = "vasya";
//
//    /** Secondary IGFS name. */
//    protected static final String SECONDARY_IGFS_NAME = "igfs-secondary";
//
//    /** Red constant. */
//    protected static final int red = 10_000;
//
//    /** Blue constant. */
//    protected static final int blue = 20_000;
//
//    /** Green constant. */
//    protected static final int green = 15_000;
//
//    /** Yellow constant. */
//    protected static final int yellow = 7_000;
//
//    /** The secondary Ignite node. */
//    protected Ignite igniteSecondary;
//
//    /** The secondary Fs. */
//    protected IgfsSecondaryFileSystem secondaryFs;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    } // initial was 3

//    /**
//     * Gets owner of a IgfsEx path.
//     * @param p The path.
//     * @return The owner.
//     */
//    private static String getOwner(final IgfsEx i, final IgfsPath p) {
//        return IgfsUserContext.doAs(USER, new IgniteOutClosure<String>() {
//            @Override public String apply() {
//                IgfsFile f = i.info(p);
//
//                assert f != null;
//
//                return f.property(IgfsUtils.PROP_USER_NAME);
//            }
//        });
//    }

//    /**
//     * Gets owner of a secondary Fs path.
//     * @param secFs The sec Fs.
//     * @param p The path.
//     * @return The owner.
//     */
//    private static String getOwnerSecondary(final IgfsSecondaryFileSystem secFs, final IgfsPath p) {
//        return IgfsUserContext.doAs(USER, new IgniteOutClosure<String>() {
//            @Override public String apply() {
//                return secFs.info(p).property(IgfsUtils.PROP_USER_NAME);
//            }
//        });
//    }

//    /**
//     * Checks owner of the path.
//     * @param p The path.
//     */
//    private void checkOwner(IgfsPath p) {
//        String ownerPrim = getOwner(igfs, p);
//        assertEquals(USER, ownerPrim);
//
//        String ownerSec = getOwnerSecondary(secondaryFs, p);
//        assertEquals(USER, ownerSec);
//    }

    /**
     * Does actual test job
     *
     * @param useNewMapper flag to use new mapper API.
     * @param useNewCombiner flag to use new combiner API.
     * @param useNewReducer flag to use new reducer API.
     */
    protected final void doTest(IgfsPath inFile, boolean useNewMapper, boolean useNewCombiner, boolean useNewReducer)
        throws Exception {
        //igfs.delete(new IgfsPath(PATH_OUTPUT), true);

        JobConf jobConf = new JobConf();

        //jobConf.set(HadoopCommonUtils.JOB_COUNTER_WRITER_PROPERTY, IgniteHadoopFileSystemCounterWriter.class.getName());

        jobConf.setUser(USER);
        //jobConf.set(IgniteHadoopFileSystemCounterWriter.COUNTER_WRITER_DIR_PROPERTY, "/xxx/${USER}/zzz");

//        // TODO: need this?
//        //To split into about 40 items for v2
//        jobConf.setInt(FileInputFormat.SPLIT_MAXSIZE, 65000);
//
//        //For v1
//        jobConf.setInt("fs.local.block.size", 65000);

//        // File system coordinates.
//        setupFileSystems(jobConf);

//        HadoopWordCount1.setTasksClasses(jobConf, !useNewMapper, !useNewCombiner, !useNewReducer);

        Job job = fillConfig(jobConf); //Job.getInstance(jobConf);
//
//        HadoopWordCount2.setTasksClasses(job, useNewMapper, useNewCombiner, useNewReducer, compressOutputSnappy());
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        FileInputFormat.setInputPaths(job, new Path(igfsScheme() + inFile.toString()));
//        FileOutputFormat.setOutputPath(job, new Path(igfsScheme() + PATH_OUTPUT));
//
//        job.setJarByClass(HadoopWordCount2.class);

        HadoopJobId jobId = new HadoopJobId(UUID.randomUUID(), 1);

        IgniteInternalFuture<?> fut = grid(0).hadoop().submit(jobId, createJobInfo(job.getConfiguration()));

        fut.get();

        //checkJobStatistics(jobId);

        //final String outFile = PATH_OUTPUT + "/" + (useNewReducer ? "part-r-" : "part-") + "00000";
    }

    private Job fillConfig(JobConf conf) throws Exception {
            Job job = Job.getInstance(conf);
            Path inputDir = new Path("/home/ivan/tera-generated");
            Path outputDir = new Path("/home/ivan/tera-sorted");
            boolean useSimplePartitioner = TeraSort.getUseSimplePartitioner(job);
            TeraInputFormat.setInputPaths(job, inputDir);
            FileOutputFormat.setOutputPath(job, outputDir);
            job.setJobName("TeraSort");
            //job.setJarByClass(TeraSort.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TeraInputFormat.class);
            job.setOutputFormatClass(TeraOutputFormat.class);
            if (useSimplePartitioner) {
                job.setPartitionerClass(TeraSort.SimplePartitioner.class);
            } else {
                long start = System.currentTimeMillis();
                Path partitionFile = new Path(outputDir,
                    TeraInputFormat.PARTITION_FILENAME);
                URI partitionUri = new URI(partitionFile.toString() +
                    "#" + TeraInputFormat.PARTITION_FILENAME);
                try {
                    TeraInputFormat.writePartitionFile(job, partitionFile);
                } catch (Throwable e) {
//                    LOG.error(e.getMessage());
//                    return -1;
                    throw new RuntimeException(e);
                }
                job.addCacheFile(partitionUri);
                long end = System.currentTimeMillis();
                System.out.println("Spent " + (end - start) + "ms computing partitions.");
                job.setPartitionerClass(TeraSort.TotalOrderPartitioner.class);
            }

            job.getConfiguration().setInt("dfs.replication", TeraSort.getOutputReplication(job));
            TeraOutputFormat.setFinalSync(job, true);
//            int ret = job.waitForCompletion(true) ? 0 : 1;

            return job;
        }

//    /**
//     * Gets if to compress output data with Snappy.
//     *
//     * @return If to compress output data with Snappy.
//     */
//    protected final boolean compressOutputSnappy() {
//        return false;
//    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(gridCount());
    }

//    /**
//     * Start grid with IGFS.
//     *
//     * @param gridName Grid name.
//     * @param igfsName IGFS name
//     * @param mode IGFS mode.
//     * @param secondaryFs Secondary file system (optional).
//     * @param restCfg Rest configuration string (optional).
//     * @return Started grid instance.
//     * @throws Exception If failed.
//     */
//    protected Ignite startGridWithIgfs(String gridName, String igfsName, IgfsMode mode,
//        @Nullable IgfsSecondaryFileSystem secondaryFs, @Nullable IgfsIpcEndpointConfiguration restCfg) throws Exception {
//        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();
//
//        igfsCfg.setDataCacheName("dataCache");
//        igfsCfg.setMetaCacheName("metaCache");
//        igfsCfg.setName(igfsName);
//        igfsCfg.setBlockSize(IGFS_BLOCK_SIZE);
//        igfsCfg.setDefaultMode(mode);
//        igfsCfg.setIpcEndpointConfiguration(restCfg);
//        igfsCfg.setSecondaryFileSystem(secondaryFs);
//        igfsCfg.setPrefetchBlocks(PREFETCH_BLOCKS);
//        igfsCfg.setSequentialReadsBeforePrefetch(SEQ_READS_BEFORE_PREFETCH);
//
//        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();
//
//        dataCacheCfg.setName("dataCache");
//        dataCacheCfg.setCacheMode(PARTITIONED);
//        dataCacheCfg.setNearConfiguration(null);
//        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
//        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(2));
//        dataCacheCfg.setBackups(0);
//        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
//        dataCacheCfg.setOffHeapMaxMemory(0);
//
//        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();
//
//        metaCacheCfg.setName("metaCache");
//        metaCacheCfg.setCacheMode(REPLICATED);
//        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
//        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);
//
//        IgniteConfiguration cfg = new IgniteConfiguration();
//
//        cfg.setGridName(gridName);
//
//        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
//
//        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));
//
//        cfg.setDiscoverySpi(discoSpi);
//        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
//        cfg.setFileSystemConfiguration(igfsCfg);
//
//        cfg.setLocalHost("127.0.0.1");
//        cfg.setConnectorConfiguration(null);
//
//        HadoopConfiguration hadoopCfg = createHadoopConfiguration();
//
//        if (hadoopCfg != null)
//            cfg.setHadoopConfiguration(hadoopCfg);
//
//        return G.start(cfg);
//    }
//
//    /**
//     * Creates custom Hadoop configuration.
//     *
//     * @return The Hadoop configuration.
//     */
//    protected HadoopConfiguration createHadoopConfiguration() {
//        return null;
//    }
//
//    /** {@inheritDoc} */
//    @Override public FileSystemConfiguration igfsConfiguration() throws Exception {
//        FileSystemConfiguration fsCfg = super.igfsConfiguration();
//
//        secondaryFs = new IgniteHadoopIgfsSecondaryFileSystem(SECONDARY_URI, SECONDARY_CFG);
//
//        fsCfg.setSecondaryFileSystem(secondaryFs);
//
//        return fsCfg;
//    }

    public void testTeraSort() throws Exception {
//        IgfsPath inDir = new IgfsPath(PATH_INPUT);
//
//        igfs.mkdirs(inDir);
//
//        IgfsPath inFile = new IgfsPath(inDir, HadoopWordCount2.class.getSimpleName() + "-input");
//
//        generateTestFile(inFile.toString(), "red", red, "blue", blue, "green", green, "yellow", yellow );
//
//        for (boolean[] apiMode: getApiModes()) {
//            assert apiMode.length == 3;
//
//            boolean useNewMapper = apiMode[0];
//            boolean useNewCombiner = apiMode[1];
//            boolean useNewReducer = apiMode[2];
//
            doTest(null, false, false, false);
//        }
    }

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration igc = super.getConfiguration(gridName);

        ((TcpCommunicationSpi)igc.getCommunicationSpi()).setConnectTimeout(30_000);
        ((TcpCommunicationSpi)igc.getCommunicationSpi()).setIdleConnectionTimeout(60_000);
        ((TcpCommunicationSpi)igc.getCommunicationSpi()).setMaxConnectTimeout(1_000_000);

        ConnectorConfiguration cc = new ConnectorConfiguration();
        cc.setIdleTimeout(30_000);
        igc.setConnectorConfiguration(cc);

        return igc;
    }

    //    protected Ignite startGridWithIgfs(String gridName, String igfsName, IgfsMode mode,
//        @Nullable IgfsSecondaryFileSystem secondaryFs, @Nullable IgfsIpcEndpointConfiguration restCfg) throws Exception {
//        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();
//
//        igfsCfg.setDataCacheName("dataCache");
//        igfsCfg.setMetaCacheName("metaCache");
//        igfsCfg.setName(igfsName);
//        igfsCfg.setBlockSize(IGFS_BLOCK_SIZE);
//        igfsCfg.setDefaultMode(mode);
//        igfsCfg.setIpcEndpointConfiguration(restCfg);
//        igfsCfg.setSecondaryFileSystem(secondaryFs);
//        igfsCfg.setPrefetchBlocks(PREFETCH_BLOCKS);
//        igfsCfg.setSequentialReadsBeforePrefetch(SEQ_READS_BEFORE_PREFETCH);
//
//        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();
//
//        dataCacheCfg.setName("dataCache");
//        dataCacheCfg.setCacheMode(PARTITIONED);
//        dataCacheCfg.setNearConfiguration(null);
//        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
//        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(2));
//        dataCacheCfg.setBackups(0);
//        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
//        dataCacheCfg.setOffHeapMaxMemory(0);
//
//        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();
//
//        metaCacheCfg.setName("metaCache");
//        metaCacheCfg.setCacheMode(REPLICATED);
//        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
//        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);
//
//        IgniteConfiguration cfg = new IgniteConfiguration();
//
//        cfg.setGridName(gridName);
//
//        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
//
//        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));
//
//        cfg.setDiscoverySpi(discoSpi);
//        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
//        cfg.setFileSystemConfiguration(igfsCfg);
//
//        cfg.setLocalHost("127.0.0.1");
//        cfg.setConnectorConfiguration(null);
//
//        HadoopConfiguration hadoopCfg = createHadoopConfiguration();
//
//        if (hadoopCfg != null)
//            cfg.setHadoopConfiguration(hadoopCfg);
//
//        return G.start(cfg);
//    }
}
