package org.sustain.etl.ncproj;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import ucar.ma2.Array;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.Math;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Command(name = "dump", mixinStandardHelpOptions = true,
    description = "reproject netcdf data using grid index")
public class DumpCommand implements Callable<Integer> {
    @Parameters(index = "0", description = "grid index file")
    private File indexFile;

    @Parameters(index = "1..*", description = "netcdf files")
    private File[] netcdfFiles;

    @Option(names = {"-n", "--no-header"}, negatable = true,
        description = "do not print csv header")
    private boolean printHeader = true;

    @Option(names = {"-b", "--buffer-size"},
        description = "time buffer size")
    private int bufferSize = 50;

    @Option(names = {"-t", "--thread-count"},
        description = "number of threads for dumping the netcdf file")
    private short threadCount = 8;

    @Override
    public Integer call() throws Exception {
        // open grid index file
        FileReader fileIn = new FileReader(this.indexFile);
        BufferedReader in = new BufferedReader(fileIn);

        // read index entries
        HashMap<String, ArrayList<int[]>> indexMap = new HashMap();
        String line = null;
        while ((line = in.readLine()) != null) {
            String[] fields = line.split(" ");
            String shapeId = fields[0];

            // parse indices
            ArrayList<int[]> indices = new ArrayList();
            for (int i = 1; i < fields.length; i++) {
                String[] indexFields = fields[i].split(":");
                int[] index = new int[]{
                    Integer.parseInt(indexFields[0]),
                    Integer.parseInt(indexFields[1])
                };

                indices.add(index);
            }

            indexMap.put(shapeId, indices);
        }

        // close grid index file readers
        in.close();
        fileIn.close();

        // identify netcdf variables
        ArrayList<ArrayList<String>> variables = new ArrayList();
        ArrayList<Float> fillValues = new ArrayList();
        long[] timeArray = null;
        int latitudeLength = 0;
        int longitudeLength = 0;

        for (File file : this.netcdfFiles) {
            // open netcdf file
            NetcdfFile netcdfFile = NetcdfFile.open(file.getPath());

            // if not done yet -> read time, lat, and long dimensions
            if (timeArray == null) {
                Variable timeVariable = netcdfFile.findVariable("time");
                // TODO - test if variable is null
                Array timeVariableArray = timeVariable.read();

                timeArray = new long[(int) timeVariableArray.getSize()];
                LocalDateTime dateTime =
                    LocalDateTime.of(1900, 1, 1, 0, 0, 0);
                for (int i = 0; i < timeArray.length; i++) {
                    timeArray[i] = dateTime
                        .plusDays(timeVariableArray.getInt(i))
                        .toEpochSecond(ZoneOffset.UTC);
                }

                Variable latVariable = netcdfFile.findVariable("lat");
                // TODO - test if variable is null
                latitudeLength = (int) latVariable.getSize();

                Variable longVariable = netcdfFile.findVariable("lon");
                // TODO - test if variable is null
                longitudeLength = (int) longVariable.getSize();
            }

            // read dimensions
            HashSet dimensions = new HashSet();
            for (Dimension dimension : netcdfFile.getDimensions()) {
                dimensions.add(dimension.getShortName());
            }

            // iterate over variables
            ArrayList arrayList = new ArrayList();
            for (Variable variable : netcdfFile.getVariables()) {
                if (!dimensions.contains(variable.getShortName())) {
                    arrayList.add(variable.getShortName());

                    Attribute attribute =
                        variable.findAttribute("_FillValue");
                    fillValues.add(attribute.getNumericValue().floatValue());
                }
            }

            variables.add(arrayList);
        }

        // print csv header
        if (this.printHeader) {
            System.out.print("gis_join,timestamp");
            for (ArrayList<String> arrayList : variables) {
                for (String variable : arrayList) {
                    System.out.print(",min_" + variable
                        + ",max_" + variable);
                }
            }
            System.out.println("");
        }

        // start worker threads
        ArrayList<Array> buffer = new ArrayList();
		AtomicLong count = new AtomicLong(0);
		ArrayList<Thread> workerThreads = new ArrayList();
		LinkedBlockingQueue<DumpOperand> queue =
            new LinkedBlockingQueue();
        ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

		for (int i = 0; i < this.threadCount; i++) {
            Thread workerThread = new Thread(
                new DumpThread(
                    buffer, count, fillValues, indexMap,
                    queue, rwLock, timeArray
                ));

			workerThread.start();
			workerThreads.add(workerThread);
		}

        // compute variable ranges
        int timeOffset = 0;
        while (true) {
            // compute size of time buffer
            int bufferSize = Math.min(this.bufferSize,
                timeArray.length - timeOffset);

            // if no more items -> break
            if (bufferSize == 0) {
                break;
            }

            // read into buffer
            int[] origin = new int[]{timeOffset, 0, 0};
            int[] section = new int[]{bufferSize,
                latitudeLength, longitudeLength};

            rwLock.writeLock().lock();
            try {
                buffer.clear();
                for (int i = 0; i < this.netcdfFiles.length; i++) {
                    // open netcdf file
                    NetcdfFile netcdfFile =
                        NetcdfFile.open(this.netcdfFiles[i].getPath());

                    // iterate over file variables
                    for (String variableName : variables.get(i)) {
                        Variable variable =
                            netcdfFile.findVariable(variableName);

                        // read section of variable
                        Array array = variable.read(origin, section);
                        buffer.add(array);
                    }
                }
            } finally {
                rwLock.writeLock().unlock();
            }

            // add evaluation operands
            for (int i = 0; i < bufferSize; i++) {
                for (String shapeId : indexMap.keySet()) {
                    queue.add(new DumpOperand(shapeId, timeOffset, i));
                    count.incrementAndGet();
                }
            }

            // wait for worker threads to complete
            while (count.get() != 0) {
                Thread.sleep(50);
            }

            // increment time offset
            timeOffset += bufferSize;
        }

        // stop worker threads
        for (Thread workerThread : workerThreads) {
            workerThread.interrupt();
        }

        return 0;
    }
}
