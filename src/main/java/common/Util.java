package common;

public class Util {
    public static void delay() {
        long start = System.nanoTime();
        // loop 0.01 ms
        while(System.nanoTime() - start < 100000) {}
    }
}
