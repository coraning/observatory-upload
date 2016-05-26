package ch.zhaw.mami.mr;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Mapper<I, O> extends Thread {

    private final LinkedBlockingQueue<I> queue;
    private final LinkedBlockingQueue<O> rqueue;
    private final MapFunction<I, O> f;
    private boolean shouldStop = false;

    public Mapper(final LinkedBlockingQueue<I> queue,
            final LinkedBlockingQueue<O> rqueue, final MapFunction<I, O> f) {
        this.queue = queue;
        this.rqueue = rqueue;
        this.f = f;
    }

    @Override
    public void run() {
        try {
            while (true) {
                if (shouldStop) {
                    if (queue.isEmpty()) {
                        break;
                    }
                }

                I value = queue.poll(2, TimeUnit.SECONDS);
                if (value == null) {
                    continue;
                }

                O o = f.f(value);
                rqueue.put(o);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-13);
        }
    }

    public void setStop() {
        shouldStop = true;
    }

}
