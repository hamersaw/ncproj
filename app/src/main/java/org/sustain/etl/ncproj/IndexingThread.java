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

public class IndexingThread implements Runnable {
    protected AtomicLong count;
    protected HashMap<String, ArrayList<int[]>> indexMap;
    protected Array latitudeArray;
    protected Array longitudeArray;
    protected float latitudeDelta;
    protected float longitudeDelta;
    protected MongoCollection mongoCollection;
    protected ReentrantReadWriteLock rwLock;
    protected LinkedBlockingQueue<int[]> queue;

    public IndexingThread(AtomicLong count,
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

        this.rwLock = rwLock;

        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                int[] datum = queue.take();

                float latitude =
                    latitudeArray.getFloat(datum[0]);
                float longitude =
                    longitudeArray.getFloat(datum[1]) - 360;

                float[][][] coordinates = {{{longitude, latitude},
                    {longitude, latitude + latitudeDelta},
                    {longitude + longitudeDelta, latitude + latitudeDelta},
                    {longitude + longitudeDelta, latitude},
                    {longitude, latitude}}};

                BasicDBObject query = new BasicDBObject("geometry",
                    new BasicDBObject("$geoIntersects",
                    new BasicDBObject("$geometry",
                        new BasicDBObject("type", "Polygon")
                            .append("coordinates", coordinates))));

                FindIterable<Document> documents =
                    mongoCollection.find(query);

                for (Document document : documents) {
                    String gisJoin = (String) document.get("gis_join");

                    this.rwLock.writeLock().lock();
                    try {
                        ArrayList<int[]> list = null;
                        if (!this.indexMap.containsKey(gisJoin)) {
                            list = new ArrayList();
                            indexMap.put(gisJoin, list);
                        } else {
                            list = this.indexMap.get(gisJoin);
                        }

                        list.add(datum);
                    } finally {
                        this.rwLock.writeLock().unlock();
                    }
                }

                count.decrementAndGet();
            }
        } catch (InterruptedException e) {}
    }
}
