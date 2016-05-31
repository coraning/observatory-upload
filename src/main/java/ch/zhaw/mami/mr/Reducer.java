package ch.zhaw.mami.mr;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Reducer<A> extends Thread {

    private A result = null;

    private LinkedBlockingQueue<A> queue;

    private boolean shouldStop = false;

    private ReduceFunction<A> f;

    public Reducer(final LinkedBlockingQueue<A> queue, final ReduceFunction<A> f) {
        this.queue = queue;
        this.f = f;
    }

    public A getResult() {
        return result;
    }

    @Override
    public void run() {

        long min = Long.MAX_VALUE, max = 0, avg = 0;

        try {
            while (true) {
                if (shouldStop && queue.isEmpty()) {
                    break;
                }

                long millis = System.currentTimeMillis();

                A a = result;
                A b = queue.poll(2, TimeUnit.SECONDS);

                if (b == null) {
                    continue;
                }

                if (a == null) {
                    result = b;
                }
                else {
                    result = f.f(a, b);
                }

                long diff = System.currentTimeMillis() - millis;

                min = Math.min(min, diff);
                max = Math.max(max, diff);
                avg = (avg + diff) / 2;
            }

            System.out.println("Reducer min/max/avg: " + min + "/" + max + "/"
                    + avg);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-23);
        }
    }

    public void setInputQueue(final LinkedBlockingQueue<A> queue) {
        this.queue = queue;
    }

    public void setReduceFunction(final ReduceFunction<A> f) {
        this.f = f;
    }

    public void setStop() {
        shouldStop = true;
    }
}
