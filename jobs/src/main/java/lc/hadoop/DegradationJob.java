package lc.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lc.hadoop.io.StatsWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
 * J4 - tool degradation curves, by joining telemetry to the maintenance log.
 *
 * A degradation curve is torque plotted against time-since-last-maintenance, not
 * against wall-clock time. Getting that x-axis requires joining every telemetry
 * sample to the most recent maintenance event for its station.
 *
 * Technique: map-side join via the distributed cache. The maintenance log is a
 * few dozen rows; telemetry is tens of millions. Shipping the small side to
 * every mapper and joining in memory avoids the shuffle entirely - a reduce-side
 * join here would push the whole telemetry table across the network to
 * accomplish nothing. Knowing which join to pick, and why, is the point.
 *
 * The reducer output is the curve: mean torque per bucket of age. The slope of
 * that curve across buckets is the station's degradation rate, and the station
 * with the steepest slope is the one whose maintenance interval should be
 * shortened.
 *
 * Configurable: -D lc.signal=torque
 *               -D lc.bucket.seconds=1800   (30-minute buckets)
 *
 * Input : telemetry;  -files or addCacheFile supplies maintenance.csv
 * Output: station \t age_bucket_h, n, mean, sd
 */
public class DegradationJob extends Configured implements Tool {

    public static final String SIGNAL_KEY = "lc.signal";
    public static final String BUCKET_KEY = "lc.bucket.seconds";

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

    public enum Rows { JOINED, NO_MAINTENANCE_YET, FILTERED_OTHER_SIGNAL, FILTERED_NOT_RUNNING, MALFORMED }

    public static class DegradationMapper extends Mapper<LongWritable, Text, Text, StatsWritable> {

        /** station -> ascending array of maintenance timestamps (ms). */
        private final Map<String, long[]> maintenance = new HashMap<String, long[]>();
        private String wanted;
        private double minValue;
        private long bucketSeconds;

        private final Text outKey = new Text();
        private final StatsWritable one = new StatsWritable();

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();
            wanted = conf.get(SIGNAL_KEY, "torque");
            minValue = conf.getDouble(MIN_VALUE_KEY, Double.NEGATIVE_INFINITY);
            bucketSeconds = conf.getLong(BUCKET_KEY, 1800L);

            URI[] cached = ctx.getCacheFiles();
            if (cached == null || cached.length == 0) {
                throw new IOException("maintenance.csv was not supplied to the distributed cache");
            }

            Map<String, List<Long>> tmp = new HashMap<String, List<Long>>();
            FileSystem fs = FileSystem.get(cached[0], conf);
            BufferedReader r = new BufferedReader(
                    new InputStreamReader(fs.open(new Path(cached[0])), StandardCharsets.UTF_8));
            try {
                String line;
                while ((line = r.readLine()) != null) {
                    if (line.isEmpty() || line.startsWith("ts_ms")) {
                        continue;
                    }
                    String[] f = line.split(",", -1);
                    if (f.length < 4) {
                        continue;
                    }
                    String sid = f[1].trim();
                    long ts = Long.parseLong(f[0].trim());
                    long dur = Long.parseLong(f[3].trim());
                    // the tool is fresh once the stop ENDS, not when it starts
                    List<Long> l = tmp.get(sid);
                    if (l == null) {
                        l = new ArrayList<Long>();
                        tmp.put(sid, l);
                    }
                    l.add(ts + dur);
                }
            } finally {
                r.close();
            }

            for (Map.Entry<String, List<Long>> e : tmp.entrySet()) {
                long[] a = new long[e.getValue().size()];
                for (int i = 0; i < a.length; i++) {
                    a[i] = e.getValue().get(i);
                }
                Arrays.sort(a);
                maintenance.put(e.getKey(), a);
            }
        }

        /** Index of the latest maintenance at or before ts, or -1 if none yet. */
        private static long lastMaintenanceAtOrBefore(long[] sorted, long ts) {
            if (sorted == null || sorted.length == 0 || ts < sorted[0]) {
                return -1L;
            }
            int lo = 0;
            int hi = sorted.length - 1;
            int best = -1;
            while (lo <= hi) {
                int mid = (lo + hi) >>> 1;
                if (sorted[mid] <= ts) {
                    best = mid;
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }
            return best < 0 ? -1L : sorted[best];
        }

        @Override
        protected void map(LongWritable offset, Text value, Context ctx)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.isEmpty() || line.startsWith("ts_ms")) {
                return;
            }

            String[] f = line.split(",", -1);
            if (f.length < 4) {
                ctx.getCounter(Rows.MALFORMED).increment(1L);
                return;
            }
            if (!wanted.equals(f[2].trim())) {
                ctx.getCounter(Rows.FILTERED_OTHER_SIGNAL).increment(1L);
                return;
            }

            try {
                long ts = Long.parseLong(f[0].trim());
                String sid = f[1].trim();
                double v = Double.parseDouble(f[3].trim());

                if (v < minValue) {
                    // machine stopped: excluding these is what keeps the slope honest
                    ctx.getCounter(Rows.FILTERED_NOT_RUNNING).increment(1L);
                    return;
                }

                long last = lastMaintenanceAtOrBefore(maintenance.get(sid), ts);
                if (last < 0L) {
                    // before this station's first maintenance: age is unknown, not zero
                    ctx.getCounter(Rows.NO_MAINTENANCE_YET).increment(1L);
                    return;
                }

                long ageSec = (ts - last) / 1000L;
                long bucket = ageSec / bucketSeconds;

                one.clear();
                one.add(v);
                outKey.set(sid + "\t" + bucket);
                ctx.write(outKey, one);
                ctx.getCounter(Rows.JOINED).increment(1L);
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

    public static class CurveReducer extends Reducer<Text, StatsWritable, Text, Text> {

        private final StatsWritable acc = new StatsWritable();
        private final Text out = new Text();
        private long bucketSeconds;

        @Override
        protected void setup(Context ctx) {
            bucketSeconds = ctx.getConfiguration().getLong(BUCKET_KEY, 1800L);
        }

        @Override
        protected void reduce(Text key, Iterable<StatsWritable> values, Context ctx)
                throws IOException, InterruptedException {

            acc.clear();
            for (StatsWritable v : values) {
                acc.merge(v);
            }

            String[] parts = key.toString().split("\t", -1);
            long bucket = Long.parseLong(parts[1]);
            double ageHours = bucket * bucketSeconds / 3600.0;

            out.set(String.format("age_h=%.2f\tn=%d\tmean=%.4f\tsd=%.4f",
                    ageHours, acc.count(), acc.mean(), acc.stddev()));
            ctx.write(new Text(parts[0]), out);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("usage: degradation <telemetry-path> <maintenance.csv> <output-path>");
            return 2;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "j4-degradation-map-side-join");
        job.setJarByClass(DegradationJob.class);

        job.setMapperClass(DegradationMapper.class);
        job.setCombinerClass(StatsCombiner.class);
        job.setReducerClass(CurveReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StatsWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        // the small side of the join, shipped to every mapper
        job.addCacheFile(new Path(args[1]).toUri());

        // Input is partitioned as dt=<date>/station=<S>/part-0.csv. FileInputFormat
        // lists only the immediate children of the input path unless recursion is
        // enabled, so without this it tries to read the dt= directories as files.
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean ok = job.waitForCompletion(true);
        if (ok) {
            System.out.printf("%nrows: joined=%,d  before-first-maintenance=%,d  "
                            + "other-signal=%,d  not-running=%,d  malformed=%,d%n",
                    job.getCounters().findCounter(Rows.JOINED).getValue(),
                    job.getCounters().findCounter(Rows.NO_MAINTENANCE_YET).getValue(),
                    job.getCounters().findCounter(Rows.FILTERED_OTHER_SIGNAL).getValue(),
                    job.getCounters().findCounter(Rows.FILTERED_NOT_RUNNING).getValue(),
                    job.getCounters().findCounter(Rows.MALFORMED).getValue());
            System.out.println("the fastest-degrading station has the steepest mean-vs-age slope");
        }
        return ok ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new DegradationJob(), args));
    }
}
