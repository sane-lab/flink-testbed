package flinkapp.StreamSluiceTestSet;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

public class DumbStatefulMap extends RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>> {

    private final int stateSize;
    private transient MapState<Integer, Long> countMap;
    private RandomDataGenerator randomGen = new RandomDataGenerator();
    DumbStatefulMap(int _stateSize){
        stateSize = _stateSize;
    }

    @Override
    public Tuple3<String, Long, Long> map(Tuple2<String, Long> input) throws Exception {
        int hashValue = (input.f0.hashCode() % stateSize + stateSize) % stateSize;
        Long cur = countMap.get(hashValue);
        cur = (cur == null) ? 1 : cur + 1;
        countMap.put(hashValue, cur);
        delay(10);
        long currentTime = System.currentTimeMillis();
        System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f1));
        return new Tuple3<String, Long, Long>(input.f0, currentTime, currentTime - input.f1);
    }
    private void delay(int interval) {
        Double ranN = randomGen.nextGaussian(interval, 1);
        ranN = ranN*1000000;
        long delay = ranN.intValue();
        if (delay < 0) delay = interval * 1000000;
        Long start = System.nanoTime();
        while (System.nanoTime() - start < delay) {}
    }

    @Override
    public void open(Configuration config) {
        MapStateDescriptor<Integer, Long> descriptor =
                new MapStateDescriptor<>("word-count", Integer.class, Long.class);
//            try {
//                if(outputStream == null) {
//                    outputStream = new FileOutputStream("/home/hya/prog/latency.out");
//                }else {
//                    System.out.println("already have an ouput stream during last open");
//                }
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            }
        countMap = getRuntimeContext().getMapState(descriptor);
    }
}
