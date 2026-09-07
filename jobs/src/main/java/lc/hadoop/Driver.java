package lc.hadoop;

import org.apache.hadoop.util.ProgramDriver;

/**
 * Single entry point for every job in the jar.
 *
 *   hadoop jar line-control-jobs.jar oee         <part_events> <out>
 *   hadoop jar line-control-jobs.jar bottleneck  <part_events> <out>
 *   hadoop jar line-control-jobs.jar limits      <telemetry>   <out>   [-D lc.signal=torque]
 *   hadoop jar line-control-jobs.jar degradation <telemetry> <maintenance.csv> <out>
 *   hadoop jar line-control-jobs.jar smoke       <input> <out>
 */
public class Driver {

    public static void main(String[] args) {
        int exit = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("oee", OeeJob.class,
                    "J1  per-station OEE from part events (combiner + custom Writable)");
            pgd.addClass("bottleneck", BottleneckJob.class,
                    "J2  find the line constraint (secondary sort)");
            pgd.addClass("limits", ControlLimitsJob.class,
                    "J3  mine SPC control limits from telemetry (stable variance)");
            pgd.addClass("degradation", DegradationJob.class,
                    "J4  tool degradation curves (map-side join via distributed cache)");
            pgd.addClass("smoke", SmokeCount.class,
                    "    day-0 gate job: records per station");
            exit = pgd.run(args);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(exit);
    }
}
