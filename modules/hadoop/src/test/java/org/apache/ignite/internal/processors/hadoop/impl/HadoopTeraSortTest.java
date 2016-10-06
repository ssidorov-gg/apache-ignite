package org.apache.ignite.internal.processors.hadoop.impl;

import java.net.URI;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraInputFormat;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraOutputFormat;
import org.apache.ignite.internal.processors.hadoop.impl.examples.terasort.TeraSort;
import org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopFileSystemsUtils;
import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.createJobInfo;

/**
 *
 */
public class HadoopTeraSortTest extends HadoopAbstractSelfTest {
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

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected final boolean igfsEnabled() {
        return false;
    }

    /** The user to run Hadoop job on behalf of. */
    protected static final String USER = "vasya";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    } // initial was 3

    /**
     * Does actual test job
     *
     * @param useNewMapper flag to use new mapper API.
     * @param useNewCombiner flag to use new combiner API.
     * @param useNewReducer flag to use new reducer API.
     */
    protected final void doTest(IgfsPath inFile, boolean useNewMapper, boolean useNewCombiner, boolean useNewReducer)
        throws Exception {
        JobConf jobConf = new JobConf();

        jobConf.setUser(USER);

        Job job = fillConfig(jobConf); //Job.getInstance(jobConf);

        HadoopJobId jobId = new HadoopJobId(UUID.randomUUID(), 1);

        IgniteInternalFuture<?> fut = grid(0).hadoop().submit(jobId, createJobInfo(job.getConfiguration()));

        fut.get();
    }

    private Job fillConfig(JobConf conf) throws Exception {
            Job job = Job.getInstance(conf);

            Path inputDir = new Path("./tera-generated/");
            Path outputDir = new Path("./tera-sorted/");

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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(gridCount());
    }

    public void testTeraSort() throws Exception {
         doTest(null, false, false, false);
    }

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration igc = super.getConfiguration(gridName);

        HadoopConfiguration hc = createHadoopConfiguration();

        igc.setHadoopConfiguration(hc);

        return igc;
    }

    protected HadoopConfiguration createHadoopConfiguration() {
        HadoopConfiguration hadoopCfg = new HadoopConfiguration();

        // See org.apache.ignite.configuration.HadoopConfiguration.DFLT_MAX_TASK_QUEUE_SIZE
        hadoopCfg.setMaxTaskQueueSize(30_000);

        //hadoopCfg.setMaxParallelTasks(); // 16 = ProcCodes * 2 -- default

        return hadoopCfg;
    }
}
