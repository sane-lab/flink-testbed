package flinkapp.frauddetection.function;

import flinkapp.frauddetection.rule.FraudOrNot;
import flinkapp.frauddetection.rule.Rule;
import flinkapp.frauddetection.transaction.PrecessedTransaction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

public class ProcessingFunction extends KeyedProcessFunction<String, PrecessedTransaction, FraudOrNot> {

    private final Rule rule;

    public ProcessingFunction(Rule rule) {
        this.rule = rule;
    }

    /**
     * Process one element from the input stream.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter
     * and also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The input value.
     * @param ctx   A {@link Context} that allows querying the timestamp of the element and getting
     *              a {@link TimerService} for registering timers and querying the time. The
     *              context is only valid during the invocation of this method, do not store it.
     * @param out   The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
     *                   to fail and may trigger recovery.
     */
    @Override
    public void processElement(PrecessedTransaction value, Context ctx, Collector<FraudOrNot> out) throws Exception {
//        long start = new Date().getTime();
//        long shift = start % 10 < 6 ? 2 : 1;
//        // long shift = 2;
//        try {
//            long now = new Date().getTime();
//            StringBuilder sb = new StringBuilder("Just to Increase Communication Overhead:");
//            while (now - start < 2 + shift) {
//                double tmp = (double) now / 97.0;
//                // sb.append(Long.toString(now)).append(' ');
//                // System.out.println(sb.toString());
//                now = new Date().getTime();
//            }
//        } catch (Exception e) {
//            // swallow interruption unless source is canceled
//        }
        out.collect(rule.isFraud(value));
    }

}
