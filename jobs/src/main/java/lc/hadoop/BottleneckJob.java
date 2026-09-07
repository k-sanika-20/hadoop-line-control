package lc.hadoop;

import java.io.IOException;

import lc.hadoop.io.StationTimeKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * J2 - find the line's constraint.
 *
 * The bottleneck of a tandem line is the station that is idle least: it never
 * waits, everyone else waits on it. Counting rows per station cannot find it -
 * in a serial line every station processes almost exactly the same number of
 * parts, so a count is the same everywhere by construction and any difference
 * is noise.
 *
 * Technique: secondary sort. The composite key is (station, timestamp); the
 * partitioner and grouping comparator use the station alone, while the sort
 * comparator uses both. The reducer therefore receives one station's cycles in
 * chronological order and can measure the longest unbroken run of blocked
 * cycles in a single streaming pass, without buffering millions of records.
 *
 * Note there is deliberately no combiner: folding values map-side would destroy
 * the per-record ordering the reducer depends on.
 *
 * Input : part_events
 * Output: station \t utilisation, idle, blocked, starved, longest blocked run
 */
public class BottleneckJob extends Configured implements Tool {

    public enum Rows { PARSED, MALFORMED }

    public static class BottleneckMapper
            extends Mapper<LongWritable, Text, StationTimeKey, Text> {

        private final StationTimeKey key = new StationTimeKey();
        private final Text val = new Text();

        @Override
        protected void map(LongWritable offset, Text value, Context ctx)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.isEmpty() || line.startsWith("ts_end_ms")) {
                return;
            }

            String[] f = line.split(",", -1);
            if (f.length < 8) {
                ctx.getCounter(Rows.MALFORMED).increment(1L);
                return;
            }

            try {
                long ts = Long.parseLong(f[0].trim());
                String sid = f[1].trim();
                long cycle = Long.parseLong(f[3].trim());
                long blocked = Long.parseLong(f[4].trim());
                long starved = Long.parseLong(f[5].trim());
                if (sid.isEmpty()) {
                    ctx.getCounter(Rows.MALFORMED).increment(1L);
                    return;
                }
                key.set(sid, ts);
                val.set(cycle + "," + blocked + "," + starved);
                ctx.write(key, val);
                ctx.getCounter(Rows.PARSED).increment(1L);
            } catch (NumberFormatException e) {
                ctx.getCounter(Rows.MALFORMED).increment(1L);
            }
        }
    }

    /** Route by station only, so one station lands entirely in one reducer. */
    public static class StationPartitioner extends Partitioner<StationTimeKey, Text> {
        @Override
        public int getPartition(StationTimeKey key, Text value, int numPartitions) {
            return (key.station().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    /** Group by station only, so one reduce() call sees the whole station. */
    public static class StationGroupingComparator extends WritableComparator {
        protected StationGroupingComparator() {
            super(StationTimeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((StationTimeKey) a).station().compareTo(((StationTimeKey) b).station());
        }
    }

    /** Sort by station then timestamp - this is what orders the values. */
    public static class StationTimeSortComparator extends WritableComparator {
        protected StationTimeSortComparator() {
            super(StationTimeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((StationTimeKey) a).compareTo((StationTimeKey) b);
        }
    }

    public static class BottleneckReducer
            extends Reducer<StationTimeKey, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void reduce(StationTimeKey key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {

            long cycles = 0L;
            long runMs = 0L;
            long blockedMs = 0L;
            long starvedMs = 0L;

            long currentBlockedRun = 0L;
            long longestBlockedRun = 0L;

            for (Text v : values) {
                String[] p = v.toString().split(",", -1);
                long cycle = Long.parseLong(p[0]);
                long blocked = Long.parseLong(p[1]);
                long starved = Long.parseLong(p[2]);

                cycles++;
                runMs += cycle;
                blockedMs += blocked;
                starvedMs += starved;

                // consecutive-blocked detection; only correct because values arrive in time order
                if (blocked > 0L) {
                    currentBlockedRun++;
                    if (currentBlockedRun > longestBlockedRun) {
                        longestBlockedRun = currentBlockedRun;
                    }
                } else {
                    currentBlockedRun = 0L;
                }
            }

            long idleMs = blockedMs + starvedMs;
            double utilisation = (runMs + idleMs) == 0L
                    ? 0.0
                    : (double) runMs / (double) (runMs + idleMs);

            outKey.set(key.station());
            outVal.set(String.format(
                    "cycles=%d\tutilisation=%.4f\tidle_s=%.1f\tblocked_s=%.1f\tstarved_s=%.1f"
                            + "\tmean_cycle_s=%.3f\tlongest_blocked_run=%d",
                    cycles, utilisation, idleMs / 1000.0, blockedMs / 1000.0,
                    starvedMs / 1000.0, runMs / 1000.0 / Math.max(1L, cycles),
                    longestBlockedRun));
            ctx.write(outKey, outVal);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: bottleneck <part_events-path> <output-path>");
            return 2;
        }

        Job job = Job.getInstance(getConf(), "j2-bottleneck");
        job.setJarByClass(BottleneckJob.class);

        job.setMapperClass(BottleneckMapper.class);
        job.setReducerClass(BottleneckReducer.class);

        job.setPartitionerClass(StationPartitioner.class);
        job.setSortComparatorClass(StationTimeSortComparator.class);
        job.setGroupingComparatorClass(StationGroupingComparator.class);

        job.setMapOutputKeyClass(StationTimeKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(3);

        // Input is partitioned as dt=<date>/station=<S>/part-0.csv. FileInputFormat
        // lists only the immediate children of the input path unless recursion is
        // enabled, so without this it tries to read the dt= directories as files.
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ok = job.waitForCompletion(true);
        if (ok) {
            System.out.printf("%nrows: parsed=%,d  malformed=%,d%n",
                    job.getCounters().findCounter(Rows.PARSED).getValue(),
                    job.getCounters().findCounter(Rows.MALFORMED).getValue());
            System.out.println("the bottleneck is the station with the HIGHEST utilisation");
        }
        return ok ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new BottleneckJob(), args));
    }
}
