package lc.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Sufficient statistics for a stream of doubles: count, mean, sum of squared
 * deviations, min and max.
 *
 * Two of these merge exactly, which is what makes a combiner mathematically
 * valid here - the reducer sees the same answer whether or not the combiner ran.
 *
 * The merge uses Chan's parallel variance formula rather than accumulating
 * sum and sum-of-squares. The naive form, var = (sumSq - sum*sum/n) / n,
 * subtracts two large and nearly equal numbers and loses most of its
 * significant digits when the mean is far from zero - for motor temperature
 * around 60 with a standard deviation near 1, that cancellation can return a
 * negative variance. This form never subtracts large quantities.
 */
public class StatsWritable implements Writable {

    private long n;
    private double mean;
    private double m2;
    private double min;
    private double max;

    public StatsWritable() {
        clear();
    }

    public static StatsWritable of(double x) {
        StatsWritable s = new StatsWritable();
        s.add(x);
        return s;
    }

    public void clear() {
        n = 0L;
        mean = 0.0;
        m2 = 0.0;
        min = Double.POSITIVE_INFINITY;
        max = Double.NEGATIVE_INFINITY;
    }

    /** Welford's online update. */
    public void add(double x) {
        n++;
        double delta = x - mean;
        mean += delta / n;
        m2 += delta * (x - mean);
        if (x < min) {
            min = x;
        }
        if (x > max) {
            max = x;
        }
    }

    /** Chan's parallel merge. Exact, and order-independent. */
    public void merge(StatsWritable other) {
        if (other.n == 0L) {
            return;
        }
        if (n == 0L) {
            n = other.n;
            mean = other.mean;
            m2 = other.m2;
            min = other.min;
            max = other.max;
            return;
        }
        long total = n + other.n;
        double delta = other.mean - mean;
        double newMean = mean + delta * ((double) other.n / (double) total);
        m2 = m2 + other.m2 + delta * delta * ((double) n * (double) other.n / (double) total);
        mean = newMean;
        n = total;
        if (other.min < min) {
            min = other.min;
        }
        if (other.max > max) {
            max = other.max;
        }
    }

    public long count() {
        return n;
    }

    public double mean() {
        return n == 0L ? 0.0 : mean;
    }

    /** Sample variance (n-1). */
    public double variance() {
        return n < 2L ? 0.0 : m2 / (double) (n - 1L);
    }

    public double stddev() {
        return Math.sqrt(variance());
    }

    public double min() {
        return n == 0L ? 0.0 : min;
    }

    public double max() {
        return n == 0L ? 0.0 : max;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(n);
        out.writeDouble(mean);
        out.writeDouble(m2);
        out.writeDouble(min);
        out.writeDouble(max);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        n = in.readLong();
        mean = in.readDouble();
        m2 = in.readDouble();
        min = in.readDouble();
        max = in.readDouble();
    }

    @Override
    public String toString() {
        return String.format("n=%d\tmean=%.4f\tsd=%.4f\tmin=%.4f\tmax=%.4f",
                n, mean(), stddev(), min(), max());
    }
}
