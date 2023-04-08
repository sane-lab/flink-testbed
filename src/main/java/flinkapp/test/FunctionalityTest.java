package flinkapp.test;

import Nexmark.sources.Util;
import common.FastZipfGenerator;
import common.MathUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.Preconditions;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static java.lang.Math.abs;
import static java.lang.Thread.sleep;
import static java.util.concurrent.CompletableFuture.runAsync;

public class FunctionalityTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        CompletableFuture futrue = new CompletableFuture();
//        String expectedValue = "the expected value";
//        CompletableFuture<String> alreadyCompleted = CompletableFuture.completedFuture(expectedValue);
//        assert (alreadyCompleted.get().equals(expectedValue));
//        System.out.println(alreadyCompleted.get());

//        System.out.printf("[%s] I am Cool\n", Thread.currentThread().getName());
//        CompletableFuture<Void> cf = CompletableFuture.runAsync(() -> {
//            System.out.printf("[%s] I am Cool\n", Thread.currentThread().getName());
//        });
//
//        CompletableFuture<String> cf1 = CompletableFuture.supplyAsync(() -> {
//            long start = System.currentTimeMillis();
//            while(System.currentTimeMillis() - start < 100) {}
//            System.out.printf("[%s] Am Awesome\n", Thread.currentThread().getName());
//            return null;
//        });
//        cf.get();

//        while (true) {
//            {
//                CompletableFuture cf = CompletableFuture.supplyAsync(() -> {
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    System.out.println();
//                    return "I am Cool";
//                }).thenAccept(msg ->
//                        System.out.printf("[%s] %s and am also Awesome\n", Thread.currentThread().getName(), msg));
//                try {
//                    cf.get();
//                } catch (Exception ex) {
//                    ex.printStackTrace(System.err);
//                }
//            }
//        }


//        int operatorIndex = 9;
//        int maxParallelism = 128;
//        int parallelism = 10;
//
//        for (operatorIndex=0; operatorIndex < 10; operatorIndex++) {
//            int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
//            int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
//            System.out.println("start: " + start + ", end: " + end + ", nNumbers: " + (end - start + 1));
//        }

//        testKeyGroupMapping();

//        testKeyRateControlled();
//        testLoadBalanceZipf(16);


        testZipfKeyRateControlled();
    }

    private static void testZipfKeyRateControlled() throws InterruptedException {
        int nKeys = 16384;
        int maxParallelism = 512;
        int parallelism = 8;
        int count = 0;
        int nTuples = parallelism * 1500;
        int rate = parallelism * 1500;
        final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        FastZipfGenerator fastZipfGenerator = new FastZipfGenerator(maxParallelism, 1, 0, 12345678);

        Set<Integer> srcKeygroups = computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, 0);
        Set<Integer> dstKeygroups = computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, parallelism - 1);

        Map<Integer, Integer> kgToexecutorMapping = new HashMap<>();
        for (int taskId = 0; taskId < parallelism; taskId++) {
            Set<Integer> assignedKeygroups = computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, taskId);
            for (int keygroup : assignedKeygroups) {
                kgToexecutorMapping.put(keygroup, taskId);
            }
        }


        HashMap<Integer, Double> probabilityMap = new HashMap<>();
        double prevValue = 0;
        for (Map.Entry<Double, Integer> kv : fastZipfGenerator.getMap().entrySet()) {
            probabilityMap.put(kv.getValue(), kv.getKey() - prevValue);
            prevValue = kv.getKey();
        }

        double x = 0;
        double y = 0;
        Set<Integer> migratableKeys = new HashSet<>();
        for (Map.Entry<Integer, Double> kv : probabilityMap.entrySet()) {
            if (kgToexecutorMapping.get(kv.getKey()) == 0) {
                x += kv.getValue();
                migratableKeys.add(kv.getKey());
            } else if (kgToexecutorMapping.get(kv.getKey()) == parallelism - 1) {
                y += kv.getValue();
            }
        }

        double optimal = (x + y) / 2;
        double prevKey = 0;
        ArrayDeque<Integer> nonMigratingKeys = new ArrayDeque<>();
        for (Double key : fastZipfGenerator.getMap().keySet()) {
            if (key >= optimal) {
                if (optimal - prevKey > key - optimal) {
                    nonMigratingKeys.add(fastZipfGenerator.getMap().get(key));
                }
                break;
            }
            nonMigratingKeys.add(fastZipfGenerator.getMap().get(key));
            prevKey = key;
        }

        migratableKeys.removeAll(nonMigratingKeys);
        System.out.println(migratableKeys);


        List<String> subKeySet;

        Map<Integer, Integer> stats = new HashMap<>();


        long emitStartTime = System.currentTimeMillis();

        // Another functionality test
        for (int i = 0; i < nKeys; i++) {
            String key = "A" + i;
            int keygroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;
            List<String> keys = keyGroupMapping.computeIfAbsent(keygroup, t -> new ArrayList<>());
            keys.add(key);
        }



        while (count < nTuples) {

            emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < rate / 20; i++) {
                subKeySet = keyGroupMapping.get(fastZipfGenerator.next());

                String key = getSubKeySetChar(count, subKeySet);

                int keygroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;

                count++;

                int statsByKey = stats.computeIfAbsent(keygroup, t -> 0);
                statsByKey++;
                stats.put(keygroup, statsByKey);
            }

            if (count % rate == 0) {
                // update the keyset
                System.out.println("++++++Key Stats: " + stats);
                Map<Integer, Integer> taskRateMap = new HashMap<>();
                for (int i = 0; i < parallelism; i++) taskRateMap.put(i, 0);
                for (int keygroup : stats.keySet()) {
                    int taskId = kgToexecutorMapping.get(keygroup);
                    taskRateMap.put(taskId, taskRateMap.get(taskId) + stats.get(keygroup));
                }
                System.out.println("++++++Task Stats: " + taskRateMap);

                for (Integer keygroup : migratableKeys) {
                    if (stats.containsKey(keygroup)) {
                        taskRateMap.put(0, taskRateMap.get(0) - stats.get(keygroup));
                        taskRateMap.put(parallelism - 1, taskRateMap.get(parallelism - 1) + stats.get(keygroup));
                    }
                }

                System.out.println("++++++new Task Stats after migration: " + taskRateMap);

//                int sum = 0;
//                for (int i = 1; i < stats.size(); i++) {
//                    sum += stats.get(i);
//                }
//                System.out.println(sum);
            }

            // Sleep for the rest of timeslice if needed
            Util.pause(emitStartTime);
        }
    }

    private static void testLoadBalanceZipf(int maxParallelism) {
        for (int skew = 0; skew < 15; skew++) {
            FastZipfGenerator fastZipfGenerator = new FastZipfGenerator(maxParallelism, skew / (double) 10, 0, 12345678);
            loadBalanceZipf(fastZipfGenerator);
        }
    }

    private static void loadBalanceZipf(FastZipfGenerator fastZipfGenerator) {
        Map<Double, Integer> map = fastZipfGenerator.getMap();
        double prevKey = 0;
        ArrayDeque<Integer> nonMigratingKeys = new ArrayDeque<>();
        for (Double key : map.keySet()) {
            if (key >= 0.5) {
                if (0.5 - prevKey > key - 0.5) {
                    nonMigratingKeys.add(map.get(key));
                }
                break;
            }
            nonMigratingKeys.add(map.get(key));
            prevKey = key;
        }

        System.out.println(nonMigratingKeys);
    }

    private static void testKeyRateControlled() throws InterruptedException {
        int[] keyProportion;
        int iterationSize;
        int nKeys = 16384;
        int maxParallelism = 8;
        final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();


        // Fix this key rate config
        int[] rateConfig = new int[]{6, 8, 2, 8, 1, 1, 1, 1};


        iterationSize = IntStream.of(rateConfig).sum();
        keyProportion = new int[iterationSize];

        int index = 0;
        for (int i = 0; i < rateConfig.length; i++) {
            int ratio = rateConfig[i];
            for (int j = 0; j < ratio; j++) {
                keyProportion[index] = i;
                index++;
            }
        }

        // Another functionality test
        for (int i = 0; i < nKeys; i++) {
            String key = "A" + i;
            int keygroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;
            List<String> keys = keyGroupMapping.computeIfAbsent(keygroup, t -> new ArrayList<>());
            keys.add(key);
        }

        List<String> subKeySet;

        Map<Integer, Integer> stats = new HashMap<>();


        long emitStartTime = System.currentTimeMillis();

        int count = 0;
        int nTuples = 14000;
        int rate = 14000;

        while (count < nTuples) {

            emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < rate / 20; i++) {
                subKeySet = keyGroupMapping.get(keyProportion[count % keyProportion.length]);

                String key = getSubKeySetChar(count, subKeySet);

                int keygroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;

                count++;

                int statsByKey = stats.computeIfAbsent(keygroup, t -> 0);
                statsByKey++;
                stats.put(keygroup, statsByKey);
            }

            if (count % rate == 0) {
                // update the keyset
                System.out.println("++++++new Key Stats: " + stats);
            }

            // Sleep for the rest of timeslice if needed
            Util.pause(emitStartTime);
        }
    }

    private static String getSubKeySetChar(int cur, List<String> subKeySet) {
        return subKeySet.get(cur % subKeySet.size());
    }

    private static void testKeyGroupMapping() {
        int maxParallelism = 512;

        Map<Integer, List<String>> cardinality = new HashMap<>();

        // Another functionality test
        for (int i = 0; i < 16384; i++) {
            String key = "A" + i;

            int keygroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;

            List<String> keys = cardinality.computeIfAbsent(keygroup, t -> new ArrayList<>());
            keys.add(key);
        }

        int stateAccessRatio = 10;
        int rate = 10000;

        int subKeyGroupSize = maxParallelism * stateAccessRatio / 100;

        List<String> subKeySet = selectKeyGroups(subKeyGroupSize, cardinality);
        int subKeySetSize = subKeySet.size();

        for (int g = 0; g < 100000; g++) {

            Map<Integer, List<String>> actualCardinality = new HashMap<>();

            for (int i = 0; i < rate; i++) {
                String key = subKeySet.get(i % subKeySetSize);
                int keygroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;
                List<String> keys = actualCardinality.computeIfAbsent(keygroup, t -> new ArrayList<>());
                keys.add(key);
            }

            if (g % 10000 == 0) {
                subKeySet = selectKeyGroups(subKeyGroupSize, cardinality);
                subKeySetSize = subKeySet.size();
                System.out.println(actualCardinality.size());
            }
        }
    }

    private static List<String> selectKeyGroups(int numAffectedTasks, Map<Integer, List<String>> newExecutorMapping) {
        numAffectedTasks = Math.min(numAffectedTasks, newExecutorMapping.size());
        List<String> selectedTasks = new ArrayList<>();
        List<Integer> allTaskID = new ArrayList<>(newExecutorMapping.keySet());
        Collections.shuffle(allTaskID);
        for (int i = 0; i < numAffectedTasks; i++) {
            selectedTasks.addAll(newExecutorMapping.get(allTaskID.get(i)));
        }
        return selectedTasks;
    }

    public static Set<Integer> computeKeyGroupRangeForOperatorIndex(
            int maxParallelism,
            int parallelism,
            int operatorIndex) {


        Preconditions.checkArgument(maxParallelism >= parallelism,
                "Maximum parallelism must not be smaller than parallelism.");

        int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
        int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;

        Set<Integer> assignedKeygroup = new HashSet<>();
        for (int i = start; i <= end; i++) {
            assignedKeygroup.add(i);
        }

        return assignedKeygroup;
    }
}
