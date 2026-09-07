package lc.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Composite key for secondary sort: partition and group by station, but sort by
 * (station, timestamp) so a reducer receives one station's cycles already in
 * chronological order.
 *
 * The point is that the reducer never has to buffer. A station accumulates
 * millions of cycle records over a 30-day run, and detecting runs of
 * consecutive blocked cycles needs them in time order - collecting them into a
 * list and sorting in the reducer would put the whole station in one JVM's heap
 * and fall over. Secondary sort pushes that ordering into the shuffle, which is
 * already sorting the data anyway.
 */
public class StationTimeKey implements WritableComparable<StationTimeKey> {

    private String station = "";
    private long tsMs;

    public StationTimeKey() {
    }

    public StationTimeKey(String station, long tsMs) {
        this.station = station;
        this.tsMs = tsMs;
    }

    public void set(String station, long tsMs) {
        this.station = station;
        this.tsMs = tsMs;
    }

    public String station() {
        return station;
    }

    public long tsMs() {
        return tsMs;
    }

    @Override
    public int compareTo(StationTimeKey o) {
        int c = station.compareTo(o.station);
        if (c != 0) {
            return c;
        }
        return Long.compare(tsMs, o.tsMs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StationTimeKey)) {
            return false;
        }
        StationTimeKey k = (StationTimeKey) o;
        return tsMs == k.tsMs && station.equals(k.station);
    }

    /**
     * Hashes on the station only. The default HashPartitioner would otherwise
     * scatter one station's records across every reducer, because the timestamp
     * is part of the key - which is exactly the bug that makes secondary sort
     * look broken. A custom partitioner in the job does the same thing
     * explicitly; this keeps the two consistent.
     */
    @Override
    public int hashCode() {
        return station.hashCode();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(station);
        out.writeLong(tsMs);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        station = in.readUTF();
        tsMs = in.readLong();
    }

    @Override
    public String toString() {
        return station + "@" + tsMs;
    }
}
