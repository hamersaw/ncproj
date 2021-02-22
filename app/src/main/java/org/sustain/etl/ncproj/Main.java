package org.sustain.etl.ncproj;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import ucar.ma2.Array;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Command(name = "ncproj", mixinStandardHelpOptions = true,
    description = "project geometries onto netcdf data")
public class Main implements Callable<Integer> {
    @Parameters(index = "0", description = "mongodb geometry database")
    private String mongoDatabase;

    @Parameters(index = "1", description = "mongodb geometry collection")
    private String mongoCollection;

    @Parameters(index = "2..*", description = "netcdf files")
    private File[] files;

    @Option(names = {"-i", "--indexing-threads"},
        description = "number of threads for indexing the netcdf file")
    private short indexThreads = 8;

    @Option(names = {"-o", "--host"}, description = "mongodb host")
    private String mongoHost = "127.0.0.1";

    @Option(names = {"-p", "--port"}, description = "mongodb port")
    private int mongoPort = 27017;

    @Override
    public Integer call() throws Exception {
        /**
         * initialize and populate index map
         */
        ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        HashMap<String, ArrayList<int[]>> indexMap = new HashMap();

        // open first netcdf file
        NetcdfFile netcdfFile =
            NetcdfFile.open(this.files[0].getPath());

        Array latitudeArray = netcdfFile.findVariable("lat").read();
        Array longitudeArray = netcdfFile.findVariable("lon").read();

        // connect to mongodb
        MongoClient mongo = new MongoClient(
            new ServerAddress(this.mongoHost, this.mongoPort));
        MongoDatabase mongoDatabase =
            mongo.getDatabase(this.mongoDatabase);
        MongoCollection mongoCollection = 
            mongoDatabase.getCollection(this.mongoCollection);

		// start indexing threads
		ArrayList<Thread> indexingThreads = new ArrayList();
		LinkedBlockingQueue<int[]> queue = new LinkedBlockingQueue();
		AtomicLong count = new AtomicLong(0);

		for (int i = 0; i < this.indexThreads; i++) {
            Thread indexingThread = new Thread(
                new IndexingThread(count, indexMap, latitudeArray,
                    longitudeArray, mongoCollection, queue, rwLock));

			indexingThread.start();
			indexingThreads.add(indexingThread);
		}

        // add all index pairs to indexing queue
        for (int i = 0; i < latitudeArray.getSize(); i++) {
        //for (int i = 0; i < 250; i++) {
            for (int j = 0; j < longitudeArray.getSize(); j++) {
            //for (int j = 0; j < 250; j++) {
				queue.add(new int[]{i, j});
				count.incrementAndGet();
			}
		}

		// wait for indexing threads to complete
		while (count.get() != 0) {
			Thread.sleep(50);
		}

		// stop indexing threads
		for (Thread indexingThread : indexingThreads) {
			indexingThread.interrupt();
		}

        /*HashSet<String> dimensions = new HashSet();
        for (Dimension dimension : netcdfFile.getDimensions()) {
            System.out.println("dimension: " + dimension.getName());
            dimensions.add(dimension.getName());
        }

        for (Variable variable : netcdfFile.getVariables()) {
            System.out.println("variable: " + variable.getName());
        }*/

        // TODO - push data
        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}
