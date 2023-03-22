package common;

import javax.naming.ldap.HasControls;
import java.io.Serializable;
import java.util.*;

public class FastZipfGenerator implements Serializable {
    private final Random random;
    private final NavigableMap<Double, Integer> map = new TreeMap<>();

    public FastZipfGenerator(int size, double skew, int offset) {
        random = new Random(0);
        computeMap(size, skew, offset);
    }

    public FastZipfGenerator(int size, double skew, int offset, int seed) {
        random = new Random(seed);
        computeMap(size, skew, offset);
        System.out.println(map);
    }

    private void computeMap(
            int size, double skew, int offset) {
        double div = 0;
        for (int i = 1; i <= size; i++) {
            div += (1 / Math.pow(i, skew));
        }
        double sum = 0;
        for (int i = 1; i <= size; i++) {
            double p = (1.0d / Math.pow(i, skew)) / div;
            sum += p;
            map.put(sum, i - 1 + offset);
        }
    }

    public int next() {
        double value = random.nextDouble();
        return map.ceilingEntry(value).getValue();
    }
    public Map<Double, Integer> getMap() {
        return map;
    }

    public void show_sample() {
        for (int i = 0; i < 100; i++) {
            System.out.println(this.next());
        }
    }

    public static void main(String[] args) {
        FastZipfGenerator fastZipfGenerator = new FastZipfGenerator(16, 1.5, 0, 12345678);
//        fastZipfGenerator.show_sample();
        Map<Integer, Integer> stats = new HashMap<>();
        for (int i = 0; i < 16; i++) {
            stats.put(i, 0);
        }
        for (int i = 0; i < 10000; i++) {
            int key = fastZipfGenerator.next();
            stats.put(key, stats.get(key) + 1);
        }

        System.out.println(stats);
    }
}
