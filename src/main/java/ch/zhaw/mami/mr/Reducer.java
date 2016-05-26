package ch.zhaw.mami.mr;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Reducer<A> extends Thread {

    private A result = null;

    private final LinkedBlockingQueue<A> queue;

    private boolean shouldStop = false;

    private final ReduceFunction<A> f;

    public Reducer(final LinkedBlockingQueue<A> queue, final ReduceFunction<A> f) {
        this.queue = queue;
        this.f = f;
    }

    public A getResult() {
        return result;
    }

    @Override
    public void run() {
        try {
            while (true) {
                if (shouldStop && queue.isEmpty()) {
                    break;
                }

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
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-23);
        }
    }

    public void setStop() {
        shouldStop = true;
    }
}
