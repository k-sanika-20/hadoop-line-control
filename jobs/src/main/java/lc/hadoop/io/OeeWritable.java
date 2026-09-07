package lc.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Sufficient statistics for OEE, carried per station.
 *
 * Everything here is additive, so a combiner can fold map-side partials without
 * changing the result. The ratios (availability, performance, quality) are
 * computed only at the very end, in the reducer - averaging ratios would be
 * wrong, which is the reason this class exists instead of emitting a
 * DoubleWritable per cycle.
 */
public class OeeWritable implements Writable {

    private long parts;
    private long good;
    private long runMs;
    private long downMs;
    private long blockedMs;
    private long starvedMs;
    private long minCycleMs;
    private long firstTsMs;
    private long lastTsMs;

    public OeeWritable() {
        clear();
    }

    public void clear() {
        parts = 0L;
        good = 0L;
        runMs = 0L;
        downMs = 0L;
        blockedMs = 0L;
        starvedMs = 0L;
        minCycleMs = Long.MAX_VALUE;
        firstTsMs = Long.MAX_VALUE;
        lastTsMs = Long.MIN_VALUE;
    }

    public void set(long tsMs, long cycleMs, long blocked, long starved, long down, boolean pass) {
        clear();
        parts = 1L;
        good = pass ? 1L : 0L;
        runMs = cycleMs;
        downMs = down;
        blockedMs = blocked;
        starvedMs = starved;
        minCycleMs = cycleMs;
        firstTsMs = tsMs;
        lastTsMs = tsMs;
    }

    public void merge(OeeWritable o) {
        parts += o.parts;
        good += o.good;
        runMs += o.runMs;
        downMs += o.downMs;
        blockedMs += o.blockedMs;
        starvedMs += o.starvedMs;
        if (o.minCycleMs < minCycleMs) {
            minCycleMs = o.minCycleMs;
        }
        if (o.firstTsMs < firstTsMs) {
            firstTsMs = o.firstTsMs;
        }
        if (o.lastTsMs > lastTsMs) {
            lastTsMs = o.lastTsMs;
        }
    }

    public long parts() {
        return parts;
    }

    public long good() {
        return good;
    }

    public long runMs() {
        return runMs;
    }

    public long downMs() {
        return downMs;
    }

    public long blockedMs() {
        return blockedMs;
    }

    public long starvedMs() {
        return starvedMs;
    }

    public long minCycleMs() {
        return minCycleMs == Long.MAX_VALUE ? 0L : minCycleMs;
    }

    /** Wall-clock span this station was observed over. */
    public long spanMs() {
        if (firstTsMs == Long.MAX_VALUE || lastTsMs == Long.MIN_VALUE) {
            return 0L;
        }
        return Math.max(1L, lastTsMs - firstTsMs);
    }

    public double availability() {
        long span = spanMs();
        return span == 0L ? 0.0 : Math.max(0.0, (double) (span - downMs) / (double) span);
    }

    /**
     * Performance against a supplied nameplate ideal cycle time, in seconds.
     *
     * Pass the equipment's rated cycle time. Do NOT fall back to the fastest
     * cycle ever observed: over ~12,000 cycles the minimum is an outlier of the
     * cycle-time noise, not a sustainable rate, and using it deflates OEE by
     * roughly 20% - a systematic error that showed up as every station being
     * off ground truth by the same factor.
     */
    public double performance(double idealCycleSeconds) {
        if (runMs <= 0L || idealCycleSeconds <= 0.0) {
            return 0.0;
        }
        return Math.min(1.0, (idealCycleSeconds * 1000.0 * parts) / (double) runMs);
    }

    /** Fallback only: best demonstrated cycle. Biased low - see above. */
    public double performance() {
        if (runMs <= 0L) {
            return 0.0;
        }
        return Math.min(1.0, (double) (minCycleMs() * parts) / (double) runMs);
    }

    public double oee(double idealCycleSeconds) {
        return availability() * performance(idealCycleSeconds) * quality();
    }

    public double quality() {
        return parts == 0L ? 0.0 : (double) good / (double) parts;
    }

    public double oee() {
        return availability() * performance() * quality();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(parts);
        out.writeLong(good);
        out.writeLong(runMs);
        out.writeLong(downMs);
        out.writeLong(blockedMs);
        out.writeLong(starvedMs);
        out.writeLong(minCycleMs);
        out.writeLong(firstTsMs);
        out.writeLong(lastTsMs);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        parts = in.readLong();
        good = in.readLong();
        runMs = in.readLong();
        downMs = in.readLong();
        blockedMs = in.readLong();
        starvedMs = in.readLong();
        minCycleMs = in.readLong();
        firstTsMs = in.readLong();
        lastTsMs = in.readLong();
    }
}
