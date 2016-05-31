package ch.zhaw.mami.mr.net;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class NetworkOutput<T> extends Thread {

    private final Socket dSocket;
    private final LinkedBlockingQueue<T> queue;

    private boolean shouldStop = false;

    public NetworkOutput(final LinkedBlockingQueue<T> queue,
            final String dhost, final int dport) throws IOException {
        dSocket = new Socket(dhost, dport);
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            OutputStream os = dSocket.getOutputStream();

            while (true) {
                if (shouldStop && queue.isEmpty()) {
                    break;
                }

                T value = queue.poll(2, TimeUnit.SECONDS);

                if (value == null) {
                    continue;
                }

                ObjectOutputStream oos = new ObjectOutputStream(os);
                oos.writeObject(value);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-998);
        }
    }

    public void setStop() {
        shouldStop = true;
    }

}
