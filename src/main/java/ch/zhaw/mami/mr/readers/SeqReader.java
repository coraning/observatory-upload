package ch.zhaw.mami.mr.readers;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;

import ch.zhaw.mami.RuntimeConfiguration;

public class SeqReader extends Thread {

    private final RuntimeConfiguration runtimeConfiguration;
    private final LinkedBlockingQueue<BytesWritable> queue;
    private final LinkedBlockingQueue<Path> pqueue;
    private int id = -1;

    private boolean shouldStop = false;

    public SeqReader(final RuntimeConfiguration runtimeConfiguration,
            final LinkedBlockingQueue<BytesWritable> queue,
            final LinkedBlockingQueue<Path> pqueue, final int id) {
        this.runtimeConfiguration = runtimeConfiguration;
        this.queue = queue;
        this.pqueue = pqueue;
        this.id = id;
    }

    @Override
    public void run() {

        try {

            FileSystem fs = runtimeConfiguration.getFileSystem();

            while (true) {

                if (shouldStop && pqueue.isEmpty()) {
                    break;
                }

                Path pt = pqueue.poll(2, TimeUnit.SECONDS);

                if (pt == null) {
                    continue;
                }

                if (!fs.exists(pt)) {
                    throw new IOException("File does not exist on HDFS!");
                }

                Path seqFile = pt;

                System.out.println("#" + id + " " + seqFile.toString());

                SequenceFile.Reader seqReader = new SequenceFile.Reader(
                        runtimeConfiguration.getFSConfiguration(),
                        SequenceFile.Reader.file(seqFile));

                BytesWritable key = new BytesWritable();
                BytesWritable value = new BytesWritable();
                while (seqReader.next(key, value)) {
                    // String keyAsStr = new String(key.getBytes(), 0,
                    // key.getLength());
                    // System.out.println(" " + keyAsStr + "(" +
                    // value.getLength()
                    // + ")");
                    queue.put(value);

                    key = new BytesWritable();
                    value = new BytesWritable();
                }

                seqReader.close();

            }

        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-99);
        }
    }

    public void setStop() {
        shouldStop = true;
    }
}
