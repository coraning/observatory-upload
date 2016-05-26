package ch.zhaw.mami.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;

import ch.zhaw.mami.RuntimeConfiguration;
import ch.zhaw.mami.mr.mappers.SizeF;
import ch.zhaw.mami.mr.reducers.SumF;

public class FolderReader extends Thread {

    public static void folderReader(final String[] args) throws IOException,
            InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "hdfs-mami");

        int limit = 4;
        int numMappers = 2;
        int numReducers = 1;

        LinkedBlockingQueue<BytesWritable> queue = new LinkedBlockingQueue<BytesWritable>(
                limit);

        List<Mapper<BytesWritable, Long>> mappers = new ArrayList<Mapper<BytesWritable, Long>>();
        List<Reducer<Long>> reducers = new ArrayList<Reducer<Long>>();

        System.out.println("Starting reader...");

        FolderReader fr = new FolderReader(RuntimeConfiguration.getInstance(),
                queue);
        fr.start();

        System.out.println("Starting mappers...");

        LinkedBlockingQueue<Long> rqueue = new LinkedBlockingQueue<Long>();

        for (int i = 0; i < numMappers; i++) {
            Mapper<BytesWritable, Long> mapper = new Mapper<BytesWritable, Long>(
                    queue, rqueue, new SizeF());
            mapper.start();
            mappers.add(mapper);
        }

        System.out.println("Starting reducers...");

        for (int i = 0; i < numReducers; i++) {
            Reducer<Long> reducer = new Reducer<Long>(rqueue, new SumF());
            reducer.start();
            reducers.add(reducer);
        }

        System.out.println("Waiting for reader....");

        fr.join();

        System.out.println("Waiting for mappers...");

        for (Mapper mapper : mappers) {
            mapper.setStop();
            mapper.join();
        }

        System.out.println("Waiting for reducers...");

        for (Reducer reducer : reducers) {
            reducer.setStop();
            reducer.join();
            System.out.println("Result: " + reducer.getResult());
        }

        System.out.println("Done!");

    }

    private final RuntimeConfiguration runtimeConfiguration;
    private final LinkedBlockingQueue<BytesWritable> queue;

    public FolderReader(final RuntimeConfiguration runtimeConfiguration,
            final LinkedBlockingQueue<BytesWritable> queue) {
        this.runtimeConfiguration = runtimeConfiguration;
        this.queue = queue;
    }

    @Override
    public void run() {

        try {
            String path = "tracebox-8000-00/warts/";

            org.apache.hadoop.fs.Path pt = new org.apache.hadoop.fs.Path(
                    runtimeConfiguration.getPathPrefix() + path);

            FileSystem fs = runtimeConfiguration.getFileSystem();

            if (!fs.exists(pt)) {
                throw new IOException("File does not exist on HDFS!");
            }

            if (!fs.isDirectory(pt)) {
                throw new IOException("File is not a directory on HDFS!");
            }

            RemoteIterator<LocatedFileStatus> files = fs.listFiles(pt, false);

            while (files.hasNext()) {
                Path seqFile = files.next().getPath();
                System.out.println(seqFile.toString());

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
            return;
        }
    }
}
