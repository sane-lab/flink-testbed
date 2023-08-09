package flinkapp.StreamSluiceTestSet;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class TwoPhaseSineSource implements SourceFunction<Tuple2<String, Long>>, CheckpointedFunction {
    private long PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, INTERMEDIATE_PERIOD, INTERVAL;
    private int count = 0;
    private volatile boolean isRunning = true;

    private transient ListState<Integer> checkpointedCount;

    public TwoPhaseSineSource(long PHASE1_TIME, long PHASE2_TIME, long INTERMEDIATE_TIME, long PHASE1_RATE, long PHASE2_RATE, long INTERMEDIATE_RATE, long INTERMEDIATE_PERIOD, long INTERVAL){
        this.PHASE1_TIME = PHASE1_TIME;
        this.PHASE2_TIME = PHASE2_TIME;
        this.INTERMEDIATE_TIME = INTERMEDIATE_TIME;
        this.PHASE1_RATE = PHASE1_RATE;
        this.PHASE2_RATE = PHASE2_RATE;
        this.INTERMEDIATE_RATE = INTERMEDIATE_RATE;
        this.INTERMEDIATE_PERIOD = INTERMEDIATE_PERIOD;
        this.INTERVAL = INTERVAL;
    }
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        this.checkpointedCount.clear();
        this.checkpointedCount.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.checkpointedCount = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("checkpointedCount", Integer.class));

        if (context.isRestored()) {
            for (Integer count : this.checkpointedCount.get()) {
                this.count = count;
            }
        }
    }

    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        // Phase 1
        long startTime = System.currentTimeMillis();
        System.out.println("Phase 1 start at: " + startTime);
        while (isRunning && System.currentTimeMillis() - startTime < PHASE1_TIME) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(Tuple2.of(getChar(count), System.currentTimeMillis()));
                count++;
            }
            if (count % (PHASE1_RATE * INTERVAL / 1000) == 0) {
                long ctime = System.currentTimeMillis();
                long nextTime = ((ctime - startTime) / INTERVAL + 1) * INTERVAL + startTime;
                if(nextTime >= ctime){
                    Thread.sleep(nextTime - ctime);
                }
            }
        }

        if (!isRunning) {
            return ;
        }

        // Intermediate Phase
        startTime = System.currentTimeMillis();
        System.out.println("Intermediate phase start at: " + startTime);
        long remainedNumber = (long)Math.floor(PHASE1_RATE * INTERVAL / 1000.0);
        long AMPLITUDE = INTERMEDIATE_RATE - PHASE1_RATE;
        while (isRunning && System.currentTimeMillis() - startTime < INTERMEDIATE_TIME) {
            long index = (System.currentTimeMillis() - startTime) / INTERVAL;
            if(remainedNumber <= 0){
                long ntime = (index + 1) * INTERVAL + startTime;
                double theta = Math.sin(Math.toRadians(index * INTERVAL * 360 / ((double)INTERMEDIATE_PERIOD) - 90));
                remainedNumber = (long)Math.floor((INTERMEDIATE_RATE + theta * AMPLITUDE) / 1000 * INTERVAL);
                long ctime = System.currentTimeMillis();
                if(ntime >= ctime) {
                    Thread.sleep(ntime - ctime);
                }
            }
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(Tuple2.of(getChar(count), System.currentTimeMillis()));
                remainedNumber --;
                count++;
            }
        }
        if (!isRunning) {
            return ;
        }

        // Phase 2
        startTime = System.currentTimeMillis();
        System.out.println("Phase 2 start at: " + startTime);
        while (isRunning && System.currentTimeMillis() - startTime < PHASE2_TIME) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(Tuple2.of(getChar(count), System.currentTimeMillis()));
                count++;
            }
            if (count % (PHASE2_RATE * INTERVAL / 1000) == 0) {
                long ctime = System.currentTimeMillis();
                long nextTime = ((ctime - startTime) / INTERVAL + 1) * INTERVAL + startTime;
                if(nextTime >= ctime){
                    Thread.sleep(nextTime - ctime);
                }
            }
        }
    }

    private static String getChar(int cur) {
        return "A" + (cur % 1000);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}