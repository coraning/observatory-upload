package ch.zhaw.mami.mr;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Mapper<I, O> extends Thread {

    private LinkedBlockingQueue<I> queue;
    private LinkedBlockingQueue<O> rqueue;
    private MapFunction<I, O> f;
    private boolean shouldStop = false;

    public Mapper(final LinkedBlockingQueue<I> queue,
            final LinkedBlockingQueue<O> rqueue, final MapFunction<I, O> f) {
        this.queue = queue;
        this.rqueue = rqueue;
        this.f = f;
    }

    @Override
    public void run() {

        long min = Long.MAX_VALUE, max = 0, avg = 0;
        try {
            while (true) {
                if (shouldStop) {
                    if (queue.isEmpty()) {
                        break;
                    }
                }

                long millis = System.currentTimeMillis();

                I value = queue.poll(1, TimeUnit.SECONDS);
                if (value == null) {
                    continue;
                }

                O o = f.f(value);
                rqueue.put(o);

                long diff = System.currentTimeMillis() - millis;

                min = Math.min(min, diff);
                max = Math.max(max, diff);
                avg = (avg + diff) / 2;
            }

            System.out.println("Mapper min/max/avg: " + min + "/" + max + "/"
                    + avg);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-13);
        }
    }

    public void setInputQueue(final LinkedBlockingQueue<I> queue) {
        this.queue = queue;
    }

    public void setMapFunction(final MapFunction<I, O> f) {
        this.f = f;
    }

    public void setOutputQueue(final LinkedBlockingQueue<O> rqueue) {
        this.rqueue = rqueue;
    }

    public void setStop() {
        shouldStop = true;
    }

}
