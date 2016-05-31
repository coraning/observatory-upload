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

import ch.zhaw.mami.RuntimeConfiguration;
import ch.zhaw.mami.mr.readers.SeqReader;

public class FolderReader extends Thread {

    public static void folderReader(final String[] args) throws IOException,
            InterruptedException, ClassNotFoundException,
            InstantiationException, IllegalAccessException {
        System.setProperty("HADOOP_USER_NAME", "hdfs-mami");

        int limit = 4;
        int numMappers = 2;
        int numReducers = 1;
        int numReaders = 1;
        String path = null;
        Class<?> mapFunctionClass = null;
        Class<?> reduceFunctionClass = null;

        if (args.length < 8) {
            System.out
                    .println("Need more arguments: <numReaders> <limit> <numMappers> <numReducers> <path> <mapFunction> <reduceFunction> ");
            return;
        }
        else {
            numReaders = Integer.parseInt(args[1]);
            limit = Integer.parseInt(args[2]);
            numMappers = Integer.parseInt(args[3]);
            numReducers = Integer.parseInt(args[4]);
            path = args[5];
            mapFunctionClass = Class.forName(args[6]);
            reduceFunctionClass = Class.forName(args[7]);
        }

        System.out.println("Readers: " + numReaders);
        System.out.println("Limit: " + limit);
        System.out.println("Mappers: " + numMappers);
        System.out.println("Reducers: " + numReducers);
        System.out.println("Path: " + path);
        System.out.println("MapFunction: "
                + mapFunctionClass.getCanonicalName());
        System.out.println("ReduceFunction: "
                + reduceFunctionClass.getCanonicalName());

        LinkedBlockingQueue<BytesWritable> queue = new LinkedBlockingQueue<BytesWritable>(
                limit);
        LinkedBlockingQueue<Path> pqueue = new LinkedBlockingQueue<Path>(
                numMappers);

        List<Mapper> mappers = new ArrayList<Mapper>();
        List<Reducer> reducers = new ArrayList<Reducer>();
        List<SeqReader> readers = new ArrayList<SeqReader>();

        System.out.println("Starting readers...");

        for (int i = 0; i < numReaders; i++) {
            SeqReader sr = new SeqReader(RuntimeConfiguration.getInstance(),
                    queue, pqueue, i);
            sr.start();
            readers.add(sr);
        }

        System.out.println("Starting mappers...");

        LinkedBlockingQueue<Long> rqueue = new LinkedBlockingQueue<Long>();

        for (int i = 0; i < numMappers; i++) {
            MapFunction f = (MapFunction) mapFunctionClass.newInstance();
            Mapper<BytesWritable, Long> mapper = new Mapper<BytesWritable, Long>(
                    queue, rqueue, f);
            mapper.start();
            mappers.add(mapper);
        }

        System.out.println("Starting reducers...");

        for (int i = 0; i < numReducers; i++) {
            ReduceFunction f = (ReduceFunction) reduceFunctionClass
                    .newInstance();
            Reducer<Long> reducer = new Reducer<Long>(rqueue, f);
            reducer.start();
            reducers.add(reducer);
        }

        System.out.println("Enumerating files...");

        RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration
                .getInstance();

        Path pt = new Path(runtimeConfiguration.getPathPrefix() + path);
        FileSystem fs = runtimeConfiguration.getFileSystem();

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(pt, false);

        while (files.hasNext()) {
            pqueue.put(files.next().getPath());
        }

        System.out.println("Waiting for readers....");

        for (SeqReader reader : readers) {
            reader.setStop();
            reader.join();
        }

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

}
