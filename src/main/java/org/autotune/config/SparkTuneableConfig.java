package org.autotune.config;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.storage.StorageLevel$;
import org.autotune.NominalParameter;
import org.autotune.NumericParameter;
import org.autotune.TuneableParameters;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * Created by KevinRoj on 21.05.17.
 */
@TuneableParameters(initRandomSearch = 4, cacheNextPoints = 1, reftryAfter = 4)
public class SparkTuneableConfig implements Serializable {
    static final long serialVersionUID = 421L;
    //from 1.2 MiB to 512 MiB, default 128 MiB
    @NumericParameter(min = 1258000, max = 671088640)
    public int maxPartitionBytes = 134217728;
    @NominalParameter(values = {"false", "true"})
    public boolean inMemoryColumnarStorageCompressed = true;
    @NumericParameter(min = 2, max = 500)
    public int shufflePartitions = 200;
    @NominalParameter(values = {"false", "true"})
    public boolean shuffleCompress = true;
    @NominalParameter(values = {"false", "true"})
    public boolean shuffleSpillCompress = true;
    @NominalParameter(values = {"false", "true"})
    public boolean broadcastCompress = true;
    @NominalParameter(values = {"false", "true"})
    public boolean rddCompress = false;
    @NumericParameter(min = 1, max = 32)
    public int defaultParallelism = 4; //default number of cores
    @NumericParameter(min = 15, max = 480)
    public int reducerMaxSizeInFlight = 48;
    @NumericParameter(min = 10, max = 480)
    public int shuffleSortBypassMergeThreshold = 200;
    @NominalParameter(values = {"lz4", "lzf", "snappy"})
    public String compressionCodec = "lz4";
    @NumericParameter(min = 1, max = 20)
    public int broadcastBlockSize = 4;
    /* JVM options */
    /*
    @NominalParameter(values = {"", " -XX:+AlwaysPreTouch "})
    public String alwaysPreTouch = "";
    @NumericParameter(min=5, max=90)
    public int initiatingHeapOccupancyPercent = 35;
    @NumericParameter(min=2, max=60)
    public int concGCThread = 20;
    */



    public SparkSession.Builder setConfig(SparkSession.Builder builder) {
        return builder
                .config("spark.sql.inMemoryColumnarStorage.compressed", this.inMemoryColumnarStorageCompressed)
                .config("spark.sql.files.maxPartitionBytes", this.maxPartitionBytes)
                .config("spark.sql.shuffle.partitions", this.shufflePartitions)
                .config("spark.shuffle.compress", this.shuffleCompress)
                .config("spark.shuffle.spill.compress", this.shuffleSpillCompress)
                .config("spark.broadcast.compress", this.broadcastCompress)
                .config("spark.rdd.compress", this.rddCompress)
                .config("spark.default.parallelism", this.defaultParallelism)
                .config("spark.reducer.maxSizeInFlight", this.reducerMaxSizeInFlight + "m")
                .config("spark.shuffle.sort.bypassMergeThreshold", this.shuffleSortBypassMergeThreshold)
                .config("spark.io.compression.codec", this.compressionCodec)
                .config("spark.broadcast.blockSize", this.broadcastBlockSize + "m")
                .config("spark.default.parallelism", this.defaultParallelism);
        //.config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=" + initiatingHeapOccupancyPercent + " -XX:ConcGCThread=" + concGCThread + alwaysPreTouch);
    }
}