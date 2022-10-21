package flinkapp;

import java.util.*;
import java.util.concurrent.ExecutionException;

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

        int maxParallelism = 512;

        Map<Integer, List<String>> cardinality = new HashMap<>();

        // Another functionality test
        for (int i = 0; i < 16384; i++) {
            String key = "A" + i;

            int keygroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;

            List<String> keys = cardinality.computeIfAbsent(keygroup, t -> new ArrayList<>());
            keys.add(key);
        }

//        System.out.println(cardinality);


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
}
