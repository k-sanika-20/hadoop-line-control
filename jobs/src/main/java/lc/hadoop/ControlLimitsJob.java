package lc.hadoop;

import java.io.IOException;

import lc.hadoop.io.StatsWritable;

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
 * J3 - mine SPC control limits from telemetry.
 *
 * Emits mean, sigma and three-sigma limits per station for one signal. These
 * limits go back into the policy file, where the edge controller uses them to
 * derate a station whose torque breaches the upper limit - so this job's output
 * changes how the line behaves.
 *
 * Filtering matters. Telemetry interleaves four signals in one file: torque
 * (~40), motor_temp (~60), vibration (~0.5) and spindle_load (~62). Pooling them
 * gives a bimodal mess and control limits that mean nothing. Filtering in the
 * mapper also drops roughly three quarters of the shuffle volume before it ever
 * hits the network.
 *
 * Technique: combiner over StatsWritable, which merges by Chan's parallel
 * variance formula. The textbook shortcut - accumulating sum and sum of squares
 * and computing (sumSq - sum^2/n)/n - is catastrophically unstable when the mean
 * is far from zero, and can return a negative variance on data like motor
 * temperature. See StatsWritable.
 *
 * Configurable: -D lc.signal=torque  (default torque)
 *               -D lc.sigma=3.0      (default 3)
 *
 * Input : telemetry (ts_ms,station,signal,value)
 * Output: station \t signal, n, mean, sd, lcl, ucl, min, max
 */
public class ControlLimitsJob extends Configured implements Tool {

    public static final String SIGNAL_KEY = "lc.signal";
    public static final String SIGMA_KEY = "lc.sigma";

    /**
     * Samples below this are treated as NOT RUNNING and excluded.
     *
     * A stopped machine still emits telemetry - near-zero torque - and those
     * readings are not part of the running process. Leaving them in inflates
     * sigma by 1.5-4x (so mined control limits are far too wide to ever fire)
     * and flattens the degradation slope by up to 2x (so mined maintenance
     * intervals come out roughly twice as long as they should be). Both errors
     * were measured; excluding non-running samples recovers 91-99% of the true
     * slope. -D lc.min.value=5.0
     */
    public static final String MIN_VALUE_KEY = "lc.min.value";

    public enum Rows { KEPT, FILTERED_OTHER_SIGNAL, FILTERED_NOT_RUNNING, MALFORMED, HEADER }

    public static class LimitMapper extends Mapper<LongWritable, Text, Text, StatsWritable> {

        private String wanted;
        private double minValue;
        private final Text station = new Text();
        private final StatsWritable one = new StatsWritable();

        @Override
        protected void setup(Context ctx) {
            wanted = ctx.getConfiguration().get(SIGNAL_KEY, "torque");
            minValue = ctx.getConfiguration().getDouble(MIN_VALUE_KEY,
                    Double.NEGATIVE_INFINITY);
        }

        @Override
        protected void map(LongWritable offset, Text value, Context ctx)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.isEmpty()) {
                return;
            }
            if (line.startsWith("ts_ms")) {
                ctx.getCounter(Rows.HEADER).increment(1L);
                return;
            }

            String[] f = line.split(",", -1);
            if (f.length < 4) {
                ctx.getCounter(Rows.MALFORMED).increment(1L);
                return;
            }

            String sid = f[1].trim();
            String signal = f[2].trim();
            if (sid.isEmpty()) {
                ctx.getCounter(Rows.MALFORMED).increment(1L);
                return;
            }
            if (!wanted.equals(signal)) {
                ctx.getCounter(Rows.FILTERED_OTHER_SIGNAL).increment(1L);
                return;
            }

            try {
                double v = Double.parseDouble(f[3].trim());
                if (v < minValue) {
                    // machine stopped: not part of the running process
                    ctx.getCounter(Rows.FILTERED_NOT_RUNNING).increment(1L);
                    return;
                }
                one.clear();
                one.add(v);
                station.set(sid);
                ctx.write(station, one);
                ctx.getCounter(Rows.KEPT).increment(1L);
            } catch (NumberFormatException e) {
                ctx.getCounter(Rows.MALFORMED).increment(1L);
            }
        }
    }

    public static class StatsCombiner extends Reducer<Text, StatsWritable, Text, StatsWritable> {

        private final StatsWritable acc = new StatsWritable();

        @Override
        protected void reduce(Text key, Iterable<StatsWritable> values, Context ctx)
                throws IOException, InterruptedException {
            acc.clear();
            for (StatsWritable v : values) {
                acc.merge(v);
            }
            ctx.write(key, acc);
        }
    }

    public static class LimitReducer extends Reducer<Text, StatsWritable, Text, Text> {

        private final StatsWritable acc = new StatsWritable();
        private final Text out = new Text();
        private double k;
        private String signal;

        @Override
        protected void setup(Context ctx) {
            k = ctx.getConfiguration().getDouble(SIGMA_KEY, 3.0);
            signal = ctx.getConfiguration().get(SIGNAL_KEY, "torque");
        }

        @Override
        protected void reduce(Text key, Iterable<StatsWritable> values, Context ctx)
                throws IOException, InterruptedException {

            acc.clear();
            for (StatsWritable v : values) {
                acc.merge(v);
            }

            double mean = acc.mean();
            double sd = acc.stddev();
            out.set(String.format(
                    "signal=%s\tn=%d\tmean=%.4f\tsd=%.4f\tlcl=%.4f\tucl=%.4f\tmin=%.4f\tmax=%.4f",
                    signal, acc.count(), mean, sd, mean - k * sd, mean + k * sd,
                    acc.min(), acc.max()));
            ctx.write(key, out);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: limits <telemetry-path> <output-path>");
            System.err.println("       -D lc.signal=torque  -D lc.sigma=3.0  -D lc.min.value=5.0");
            return 2;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "j3-control-limits-" + conf.get(SIGNAL_KEY, "torque"));
        job.setJarByClass(ControlLimitsJob.class);

        job.setMapperClass(LimitMapper.class);
        job.setCombinerClass(StatsCombiner.class);
        job.setReducerClass(LimitReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StatsWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        // Input is partitioned as dt=<date>/station=<S>/part-0.csv. FileInputFormat
        // lists only the immediate children of the input path unless recursion is
        // enabled, so without this it tries to read the dt= directories as files.
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ok = job.waitForCompletion(true);
        if (ok) {
            long kept = job.getCounters().findCounter(Rows.KEPT).getValue();
            long dropped = job.getCounters().findCounter(Rows.FILTERED_OTHER_SIGNAL).getValue();
            long stopped = job.getCounters().findCounter(Rows.FILTERED_NOT_RUNNING).getValue();
            System.out.printf("%nrows: kept=%,d  other-signal=%,d  not-running=%,d  malformed=%,d%n",
                    kept, dropped, stopped,
                    job.getCounters().findCounter(Rows.MALFORMED).getValue());
            if (kept + dropped > 0L) {
                System.out.printf("mapper-side filtering removed %.1f%% of the shuffle%n",
                        100.0 * dropped / (kept + dropped));
            }
        }
        return ok ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ControlLimitsJob(), args));
    }
}
