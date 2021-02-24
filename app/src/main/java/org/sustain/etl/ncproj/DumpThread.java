package org.sustain.etl.ncproj;

import ucar.ma2.Array;
import ucar.ma2.Index3D;

import java.lang.Math;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DumpThread implements Runnable {
    protected ArrayList<Array> buffer;
    protected AtomicLong count;
    protected ArrayList<Float> fillValues;
    protected HashMap<String, ArrayList<int[]>> indexMap;
    protected LinkedBlockingQueue<DumpOperand> queue;
    protected ReentrantReadWriteLock rwLock;
    protected long[] timeArray;

    public DumpThread(ArrayList<Array> buffer, AtomicLong count,
            ArrayList<Float> fillValues,
            HashMap<String, ArrayList<int[]>> indexMap,
            LinkedBlockingQueue<DumpOperand> queue,
            ReentrantReadWriteLock rwLock, long[] timeArray) {
        this.buffer = buffer;
        this.count = count;
        this.fillValues = fillValues;
        this.indexMap = indexMap;
        this.queue = queue;
        this.rwLock = rwLock;
        this.timeArray = timeArray;
    }

    @Override
    public void run() {
        try {
            while (true) {
                // retrieve next operand
                DumpOperand operand = queue.take();

                int timeIndex = operand.getTimeOffset()
                    + operand.getTimeIndex()
                String line = operand.getShapeId()
                    + "," + timeArray[timeIndex];

                this.rwLock.readLock().lock();
                try {
                    ArrayList<int[]> indices = 
                        this.indexMap.get(operand.getShapeId());

                    // iterate over buffers
                    for (int i = 0; i < this.buffer.size(); i++) {
                        Array array = this.buffer.get(i);
                        int[] shape = array.getShape();

                        Float fillValue = this.fillValues.get(i);

                        // iterate over indices
                        float min = Float.MAX_VALUE;
                        float max = -Float.MAX_VALUE;
                        for (int[] coordinates : indices) {
                            // identify index value
                            int arrayIndex = (operand.getTimeIndex() 
                                    * shape[1] * shape[2])
                                + (coordinates[0] * shape[2])
                                + coordinates[1];

                            float value = array.getFloat(arrayIndex);

                            // skip value if fill
                            if (value == fillValue) {
                                continue;
                            }

                            // compare with min / max
                            min = Math.min(min, value);
                            max = Math.max(max, value);
                        }

                        line += "," + String.format("%.3f", min)
                            + "," + String.format("%.3f", max);
                    }
                } finally {
                    this.rwLock.readLock().unlock();
                }

                System.out.println(line);

                // decrement active count
                count.decrementAndGet();
            }
        } catch (InterruptedException e) {}
    }
}
