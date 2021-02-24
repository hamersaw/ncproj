package org.sustain.etl.ncproj;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Command(name = "index", mixinStandardHelpOptions = true,
    description = "compute shape to grid index")
public class IndexCommand implements Callable<Integer> {
    @Parameters(index = "0", description = "mongodb geometry database")
    private String mongoDatabase;

    @Parameters(index = "1",
        description = "mongodb geometry collection")
    private String mongoCollection;

    @Parameters(index = "2", description = "netcdf file")
    private File netcdfFile;

    @Option(names = {"-t", "--thread-count"},
        description = "number of threads for indexing the netcdf file")
    private short threadCount = 8;

    @Option(names = {"-o", "--host"}, description = "mongodb host")
    protected static String mongoHost = "127.0.0.1";

    @Option(names = {"-p", "--port"}, description = "mongodb port")
    protected static int mongoPort = 27017;

    @Override
    public Integer call() throws Exception {
        ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        HashMap<String, ArrayList<int[]>> indexMap = new HashMap();

        // open first netcdf file
        NetcdfFile netcdfFile =
            NetcdfFile.open(this.netcdfFile.getPath());

        Array latitudeArray = netcdfFile.findVariable("lat").read();
        Array longitudeArray = netcdfFile.findVariable("lon").read();

        // connect to mongodb
        MongoClient mongo = new MongoClient(
            new ServerAddress(this.mongoHost, this.mongoPort));
        MongoDatabase mongoDatabase =
            mongo.getDatabase(this.mongoDatabase);
        MongoCollection mongoCollection = 
            mongoDatabase.getCollection(this.mongoCollection);

		// start index threads
		ArrayList<Thread> workerThreads = new ArrayList();
		LinkedBlockingQueue<int[]> queue = new LinkedBlockingQueue();
		AtomicLong count = new AtomicLong(0);

		for (int i = 0; i < this.threadCount; i++) {
            Thread workerThread = new Thread(
                new IndexThread(count, indexMap, latitudeArray,
                    longitudeArray, mongoCollection, queue, rwLock));

			workerThread.start();
			workerThreads.add(workerThread);
		}

        // add all index pairs to indexing queue
        for (int i = 0; i < latitudeArray.getSize(); i++) {
            for (int j = 0; j < longitudeArray.getSize(); j++) {
				queue.add(new int[]{i, j});
				count.incrementAndGet();
			}
		}

		// wait for index threads to complete
		while (count.get() != 0) {
			Thread.sleep(50);
		}

		// stop index threads
		for (Thread workerThread : workerThreads) {
			workerThread.interrupt();
		}

        // write index to stdout
        for (Map.Entry<String, ArrayList<int[]>> entry :
                indexMap.entrySet()) {
            System.out.print(entry.getKey());
            for (int[] values : entry.getValue()) {
                System.out.print(" " + values[0] + ":" + values[1]);
            }
            System.out.println("");
        }

        return 0;
    }
}
