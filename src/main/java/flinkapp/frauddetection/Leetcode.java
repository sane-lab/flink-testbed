package flinkapp.frauddetection;

import java.util.*;

public class Leetcode {

    static class Solution {
        public List<Integer> largestDivisibleSubset(int[] nums) {
            Arrays.sort(nums);
            List<Integer> ans = new LinkedList<>();
            for(int i=0;i<nums.length;i++){
                LinkedList<Integer> curr = new LinkedList<>();
                if(ans.contains(nums[i])){
                    continue;
                }
                curr.add(nums[i]);
                for(int j=i+1;j<nums.length;j++){
                    if(nums[j] % curr.getLast()==0){
                        curr.add(nums[j]);
                    }

                }
                if(curr.size() > ans.size()){
                    ans = curr;
                }
            }
            return ans;
        }
    }

    public static void main(String[] args) {
        Solution s = new Solution();
        int[] in = {5,9,18,54,108,540,90,180,360,720};
        System.out.println(s.largestDivisibleSubset(in));
    }
}
