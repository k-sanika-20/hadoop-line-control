package lc.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Day 0 gate job: count records per station from a CSV in HDFS.
 *
 * Deliberately small, but it is not a toy - it establishes the three things every
 * later job depends on: a ToolRunner driver (so -D options work from the command
 * line), a combiner that is valid because the operation is associative, and
 * counters that make bad input visible instead of silently dropped.
 *
 * Input line format:  timestamp,station_id,signal,value
 */
public class SmokeCount extends Configured implements Tool {

    public enum Records { GOOD, MALFORMED }

    public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private static final LongWritable ONE = new LongWritable(1L);
        private final Text station = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.isEmpty() || line.startsWith("timestamp")) {
                return; // header
            }

            String[] f = line.split(",", -1);
            if (f.length < 4 || f[1].isEmpty()) {
                ctx.getCounter(Records.MALFORMED).increment(1L);
                return;
            }

            ctx.getCounter(Records.GOOD).increment(1L);
            station.set(f[1]);
            ctx.write(station, ONE);
        }
    }

    /**
     * Used as both combiner and reducer. Summing is associative and commutative,
     * so running it map-side first is mathematically identical and cuts shuffle volume.
     */
    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable out = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context ctx)
                throws IOException, InterruptedException {

            long total = 0L;
            for (LongWritable v : values) {
                total += v.get();
            }
            out.set(total);
            ctx.write(key, out);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: SmokeCount <input-path> <output-path>");
            return 2;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "smoke-count-records-per-station");
        job.setJarByClass(SmokeCount.class);

        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ok = job.waitForCompletion(true);
        if (ok) {
            long good = job.getCounters().findCounter(Records.GOOD).getValue();
            long bad = job.getCounters().findCounter(Records.MALFORMED).getValue();
            System.out.printf("%nrecords: %,d good / %,d malformed%n", good, bad);
        }
        return ok ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new SmokeCount(), args));
    }
}
