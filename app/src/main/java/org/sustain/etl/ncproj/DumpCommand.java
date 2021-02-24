package org.sustain.etl.ncproj;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import ucar.ma2.Array;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.Math;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.Callable;

@Command(name = "dump", mixinStandardHelpOptions = true,
    description = "reproject netcdf data using grid index")
public class DumpCommand implements Callable<Integer> {
    @Parameters(index = "0", description = "grid index file")
    private File indexFile;

    @Parameters(index = "1..*", description = "netcdf files")
    private File[] netcdfFiles;

    @Option(names = {"-b", "--buffer-size"},
        description = "time buffer size")
    private int bufferSize = 50;

    @Override
    public Integer call() throws Exception {
        /**
         * parse grid index file
         */

        // open grid index file
        FileReader fileIn = new FileReader(this.indexFile);
        BufferedReader in = new BufferedReader(fileIn);

        // read index entries
        TreeMap<String, ArrayList<int[]>> indexMap = new TreeMap();
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

        /**
         * identify netcdf variables
         */

        ArrayList<ArrayList<String>> variables = new ArrayList();
        Array timeArray = null;
        int latitudeLength = 0;
        int longitudeLength = 0;

        for (File file : this.netcdfFiles) {
            // open netcdf file
            NetcdfFile netcdfFile = NetcdfFile.open(file.getPath());

            // if not done yet -> read time, lat, and long dimensions
            if (timeArray == null) {
                Variable timeVariable = netcdfFile.findVariable("time");
                // TODO - test if variable is null
                timeArray = timeVariable.read();

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
                }
            }

            variables.add(arrayList);
        }

        /*// TODO - tmp print variables
        for (String variable : variables) {
            System.out.println(variable);
        }*/

        /**
         * compute variable ranges
         */

        ArrayList<Array> buffer = new ArrayList();
        int timeIndex = 0;

        while (true) {
            // compute size of time buffer
            int bufferSize = Math.min(this.bufferSize,
                (int) timeArray.getSize() - timeIndex);

            System.out.println(timeIndex + " " + bufferSize);

            // if no more items -> break
            if (bufferSize == 0) {
                break;
            }

            // read into buffer
            int[] origin = new int[]{timeIndex, 0, 0};
            int[] section = new int[]{bufferSize,
                latitudeLength, longitudeLength};

            buffer.clear();
            for (int i = 0; i < this.netcdfFiles.length; i++) {
                // open netcdf file
                NetcdfFile netcdfFile =
                    NetcdfFile.open(this.netcdfFiles[i].getPath());

                // iterate over file variables
                for (String variableName : variables.get(i)) {
                    Variable variable =
                        netcdfFile.findVariable(variableName);

                    /*int[] shape = variable.getShapeAll();
                    System.out.print("[");
                    for (int dimension : shape) {
                        System.out.print(" " + dimension);
                    }
                    System.out.println(" ]");*/

                    Array array = variable.read(origin, section);
                    /*System.out.println("  " + variableName
                        + " : " + array.getSize());*/

                    buffer.add(array);
                }
            }

            // TODO - compute values

            // increment timeIndex
            timeIndex += bufferSize;
        }

        return 0;
    }
}
