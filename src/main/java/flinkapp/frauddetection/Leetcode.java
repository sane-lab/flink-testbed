package flinkapp.frauddetection;

import java.util.*;

public class Leetcode {

    private static Leetcode LEETCODE = new Leetcode();

    class Solution {

        private int[] dp = new int[10001];

        public int deleteAndEarn(int[] nums) {
            Map<Integer, Integer> map = new HashMap<>();
            for(int num: nums){
                int exist = map.getOrDefault(num, 0);
                map.put(num, exist + 1);
            }
            List<Integer> keys = new ArrayList<>(map.keySet());
            Collections.sort(keys);
            int res = 0;
            for(int k: keys){
                int kres = Math.max(
                        map.get(k) * k + map.getOrDefault(k-2, 0),
                        map.getOrDefault(k-1, 0)
                );
                map.put(k, kres);
                res = kres;
            }
            return res;
        }

    }


    public static void main(String[] args) {
        Solution s = LEETCODE.new Solution();
        int[] in = {1,1,1,2,4,5,5,5,6};
        System.out.println(s.deleteAndEarn(in));
    }
}
