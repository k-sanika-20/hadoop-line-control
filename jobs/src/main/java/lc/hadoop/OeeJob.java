package lc.hadoop;

import java.io.IOException;

import lc.hadoop.io.OeeWritable;

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
 * J1 - Overall Equipment Effectiveness per station.
 *
 *   OEE = Availability x Performance x Quality
 *
 * Reads part_events, not telemetry: availability needs downtime and performance
 * needs cycle times, and neither exists in a stream of sensor samples.
 *
 * Technique: combiner over a custom Writable carrying sufficient statistics.
 * The combiner is valid because every field it folds is additive; the ratios are
 * formed once, in the reducer. Averaging per-cycle ratios would be a different
 * and wrong number.
 *
 * Input : ts_end_ms,station,part_id,cycle_ms,blocked_ms,starved_ms,down_ms,status,defect_code
 * Output: station \t parts, good, oee, availability, performance, quality, defect_rate
 */
public class OeeJob extends Configured implements Tool {

    public enum Rows { PARSED, MALFORMED, HEADER, NO_IDEAL_CYCLE_CONFIGURED }

    /** Nameplate cycle time per station, e.g. -D lc.ideal.S01=11.0 */
    public static final String IDEAL_PREFIX = "lc.ideal.";

    public static class OeeMapper extends Mapper<LongWritable, Text, Text, OeeWritable> {

        private final Text station = new Text();
        private final OeeWritable stats = new OeeWritable();

        @Override
        protected void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.isEmpty()) {
                return;
            }
            if (line.startsWith("ts_end_ms")) {
                ctx.getCounter(Rows.HEADER).increment(1L);
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
                long down = Long.parseLong(f[6].trim());
                boolean pass = "PASS".equals(f[7].trim());

                if (sid.isEmpty() || cycle <= 0L) {
                    ctx.getCounter(Rows.MALFORMED).increment(1L);
                    return;
                }

                stats.set(ts, cycle, blocked, starved, down, pass);
                station.set(sid);
                ctx.write(station, stats);
                ctx.getCounter(Rows.PARSED).increment(1L);
            } catch (NumberFormatException e) {
                ctx.getCounter(Rows.MALFORMED).increment(1L);
            }
        }
    }

    /** Folds partials. Used both map-side as the combiner and reduce-side. */
    public static class OeeCombiner extends Reducer<Text, OeeWritable, Text, OeeWritable> {

        private final OeeWritable acc = new OeeWritable();

        @Override
        protected void reduce(Text key, Iterable<OeeWritable> values, Context ctx)
                throws IOException, InterruptedException {
            acc.clear();
            for (OeeWritable v : values) {
                acc.merge(v);
            }
            ctx.write(key, acc);
        }
    }

    public static class OeeReducer extends Reducer<Text, OeeWritable, Text, Text> {

        private final OeeWritable acc = new OeeWritable();
        private final Text out = new Text();

        @Override
        protected void reduce(Text key, Iterable<OeeWritable> values, Context ctx)
                throws IOException, InterruptedException {

            acc.clear();
            for (OeeWritable v : values) {
                acc.merge(v);
            }

            // Nameplate ideal cycle time for this station, if configured.
            double ideal = ctx.getConfiguration().getDouble(IDEAL_PREFIX + key.toString(), -1.0);
            double perf;
            double oee;
            if (ideal > 0.0) {
                perf = acc.performance(ideal);
                oee = acc.oee(ideal);
            } else {
                ctx.getCounter(Rows.NO_IDEAL_CYCLE_CONFIGURED).increment(1L);
                ideal = acc.minCycleMs() / 1000.0;
                perf = acc.performance();
                oee = acc.oee();
            }

            out.set(String.format(
                    "parts=%d\tgood=%d\toee=%.4f\tavailability=%.4f\tperformance=%.4f"
                            + "\tquality=%.4f\tdefect_rate=%.5f\tideal_cycle_s=%.3f"
                            + "\tmin_cycle_s=%.3f\tdown_s=%.1f\tblocked_s=%.1f\tstarved_s=%.1f",
                    acc.parts(), acc.good(), oee, acc.availability(), perf,
                    acc.quality(), 1.0 - acc.quality(), ideal, acc.minCycleMs() / 1000.0,
                    acc.downMs() / 1000.0, acc.blockedMs() / 1000.0, acc.starvedMs() / 1000.0));
            ctx.write(key, out);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: oee <part_events-path> <output-path>");
            return 2;
        }

        Job job = Job.getInstance(getConf(), "j1-oee");
        job.setJarByClass(OeeJob.class);

        job.setMapperClass(OeeMapper.class);
        job.setCombinerClass(OeeCombiner.class);
        job.setReducerClass(OeeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OeeWritable.class);
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
            long noIdeal = job.getCounters().findCounter(Rows.NO_IDEAL_CYCLE_CONFIGURED).getValue();
            if (noIdeal > 0L) {
                System.out.printf("%nWARNING: %d station(s) had no -D %s<station> configured; "
                        + "fell back to best-observed cycle, which biases OEE low.%n",
                        noIdeal, IDEAL_PREFIX);
            }
            System.out.printf("%nrows: parsed=%,d  malformed=%,d  headers=%,d%n",
                    job.getCounters().findCounter(Rows.PARSED).getValue(),
                    job.getCounters().findCounter(Rows.MALFORMED).getValue(),
                    job.getCounters().findCounter(Rows.HEADER).getValue());
        }
        return ok ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new OeeJob(), args));
    }
}
