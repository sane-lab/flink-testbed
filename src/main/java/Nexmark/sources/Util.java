package Nexmark.sources;

import java.util.*;

public class Util {
    public void changeRate(int rate, Boolean inc, Integer n) {
        if (inc) {
            rate += n;
        } else {
            if (rate > n) {
                rate -= n;
            }
        }
    }

    public static int changeRateSin(int rate, int cycle, int epoch) {
        double sineValue = Math.sin(Math.toRadians(epoch*360/cycle)) + 1;
        System.out.println(sineValue);

        Double curRate = (sineValue * rate);
        return curRate.intValue();
    }

    public static int changeRateCos(int rate, int cycle, int epoch) {
        double sineValue = Math.cos(Math.toRadians(epoch*360/cycle)) + 1;
        System.out.println(sineValue);

        Double curRate = (sineValue * rate);
        return curRate.intValue();
    }

    public static void pause(long emitStartTime) throws InterruptedException {
        long emitTime = System.currentTimeMillis() - emitStartTime;
        if (emitTime < 1000/20) {
            Thread.sleep(1000/20 - emitTime);
        }
    }

    public static List<String> selectKeys(int subKeyGroupSize, Map<Integer, List<String>> keyGroupMapping) {
        subKeyGroupSize = Math.min(subKeyGroupSize, keyGroupMapping.size());
        List<String> selectedKeys = new ArrayList<>();
        List<Integer> allKeyGroups = new ArrayList<>(keyGroupMapping.keySet());
        Collections.shuffle(allKeyGroups);
        for (int i = 0; i < subKeyGroupSize; i++) {
            selectedKeys.addAll(keyGroupMapping.get(allKeyGroups.get(i)));
        }
        return selectedKeys;
    }

    public static Set<Integer> selectKeyGroups(int subKeyGroupSize, Map<Integer, List<String>> keyGroupMapping) {
        subKeyGroupSize = Math.min(subKeyGroupSize, keyGroupMapping.size());
        Set<Integer> selectedKeyGroups = new HashSet<>();
        List<Integer> allKeyGroups = new ArrayList<>(keyGroupMapping.keySet());
        Collections.shuffle(allKeyGroups);
        for (int i = 0; i < subKeyGroupSize; i++) {
            selectedKeyGroups.add(allKeyGroups.get(i));
        }
        return selectedKeyGroups;
    }

    public static Set<Integer> selectKeyGroups(int subKeyGroupSize, Map<Integer, List<String>> keyGroupMapping, Random random) {
        subKeyGroupSize = Math.min(subKeyGroupSize, keyGroupMapping.size());
        Set<Integer> selectedKeyGroups = new HashSet<>();
        List<Integer> allKeyGroups = new ArrayList<>(keyGroupMapping.keySet());
        Collections.shuffle(allKeyGroups, random);
        for (int i = 0; i < subKeyGroupSize; i++) {
            selectedKeyGroups.add(allKeyGroups.get(i));
        }
        return selectedKeyGroups;
    }
}
