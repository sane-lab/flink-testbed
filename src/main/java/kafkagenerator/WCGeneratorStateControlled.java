package kafkagenerator;

import Nexmark.sources.Util;
import common.MathUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

/**
 * SSE generaor
 */
public class WCGeneratorStateControlled {

    private final String TOPIC = "words";

    private static KafkaProducer<String, String> producer;

    private long SENTENCE_NUM = 1000000000;
    private int uniformSize = 10000;
    private double mu = 10;
    private double sigma = 1;

    private int count = 0;

    private transient ListState<Integer> checkpointedCount;

    private int runtime;
    private int nTuples;
    private int nKeys;
    private int rate;

    private final int subKeyGroupSize;

    private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

    public WCGeneratorStateControlled(int runtime, int nTuples, int nKeys, int maxParallelism, int stateAccessRatio) {
        this.runtime = runtime;
        this.nTuples = nTuples;
        this.nKeys = nKeys;
        this.rate = nTuples / runtime;
        this.subKeyGroupSize = maxParallelism * stateAccessRatio / 100;

        // Another functionality test
        for (int i = 0; i < nKeys; i++) {
            String key = "A" + i;
            int keygroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;
            List<String> keys = keyGroupMapping.computeIfAbsent(keygroup, t -> new ArrayList<>());
            keys.add(key);
        }

        System.out.println("runtime: " + runtime
                + ", nTuples: " + nTuples
                + ", nKeys: " + nKeys
                + ", rate: " + rate);
        initProducer();
    }

    private void initProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        producer = new KafkaProducer<>(props);
    }

    public void generate() throws InterruptedException {
        List<String> subKeySet = Util.selectKeys(subKeyGroupSize, keyGroupMapping);

        long emitStartTime;

        while (count < nTuples) {
            emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < rate / 20; i++) {
                String key = getSubKeySetChar(count, subKeySet);
                ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, System.currentTimeMillis() + ":" + key);
                producer.send(newRecord);
                count++;
            }

            // Sleep for the rest of timeslice if needed
            Util.pause(emitStartTime);

            if (count % rate == 0) {
                // update the keyset
                subKeySet = Util.selectKeys(subKeyGroupSize, keyGroupMapping);
            }
        }

        producer.close();
    }

    private String getChar(int cur) {
        return "A" + (cur % nKeys);
    }

    private String getSubKeySetChar(int cur, List<String> subKeySet) {
        return subKeySet.get(cur % subKeySet.size());
    }

    public static void main(String[] args) throws InterruptedException {
        final ParameterTool params = ParameterTool.fromArgs(args);

        new WCGeneratorStateControlled(
                params.getInt("runtime", 10),
                params.getInt("nTuples", 10000),
                params.getInt("nKeys", 1000),
                params.getInt("mp2", 128),
                params.getInt("stateAccessRatio", 25))
                .generate();
    }
}