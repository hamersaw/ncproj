package org.sustain.etl.ncproj;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

import ucar.ma2.Array;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IndexThread implements Runnable {
    protected AtomicLong count;
    protected HashMap<String, ArrayList<int[]>> indexMap;
    protected Array latitudeArray;
    protected Array longitudeArray;
    protected float latitudeDelta;
    protected float longitudeDelta;
    protected MongoCollection mongoCollection;
    protected LinkedBlockingQueue<int[]> queue;
    protected ReentrantReadWriteLock rwLock;

    public IndexThread(AtomicLong count,
            HashMap<String, ArrayList<int[]>> indexMap,
            Array latitudeArray, Array longitudeArray,
            MongoCollection mongoCollection,
            LinkedBlockingQueue<int[]> queue,
            ReentrantReadWriteLock rwLock) {
        this.count = count;

        this.indexMap = indexMap;

        this.latitudeArray = latitudeArray;
        this.longitudeArray = longitudeArray;
        this.latitudeDelta =
            latitudeArray.getFloat(1) - latitudeArray.getFloat(0);
        this.longitudeDelta =
            longitudeArray.getFloat(1) - longitudeArray.getFloat(0);

        this.mongoCollection = mongoCollection;

        this.queue = queue;

        this.rwLock = rwLock;
    }

    @Override
    public void run() {
        try {
            while (true) {
                // retrieve next index
                int[] operand = queue.take();

                // compute latitude and longitude bounds
                float latitude =
                    latitudeArray.getFloat(operand[0]);
                float longitude =
                    longitudeArray.getFloat(operand[1]) - 360;

                float[][][] coordinates = {{{longitude, latitude},
                    {longitude, latitude + latitudeDelta},
                    {longitude + longitudeDelta, latitude + latitudeDelta},
                    {longitude + longitudeDelta, latitude},
                    {longitude, latitude}}};

                // construct mongodb query object
                BasicDBObject query = new BasicDBObject("geometry",
                    new BasicDBObject("$geoIntersects",
                    new BasicDBObject("$geometry",
                        new BasicDBObject("type", "Polygon")
                            .append("coordinates", coordinates))));

                // iterate over query results
                FindIterable<Document> documents =
                    mongoCollection.find(query);

                for (Document document : documents) {
                    String gisJoin = (String) document.get("gis_join");

                    // add index coordinates to index map
                    this.rwLock.writeLock().lock();
                    try {
                        ArrayList<int[]> list = null;
                        if (!this.indexMap.containsKey(gisJoin)) {
                            list = new ArrayList();
                            indexMap.put(gisJoin, list);
                        } else {
                            list = this.indexMap.get(gisJoin);
                        }

                        list.add(operand);
                    } finally {
                        this.rwLock.writeLock().unlock();
                    }
                }

                // decrement active count
                count.decrementAndGet();
            }
        } catch (InterruptedException e) {}
    }
}
