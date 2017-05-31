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
    @NominalParameter(values = {"false", "true"})
    public boolean defaultCache = true;


    public SparkSession.Builder setConfig(SparkSession.Builder builder) {

        try {
            if (!this.defaultCache) {
                Field fieldX = StorageLevel$.class.getDeclaredField("MEMORY_AND_DISK");
                fieldX.setAccessible(true);
                fieldX.set(StorageLevel$.MODULE$, StorageLevel.MEMORY_ONLY_SER());
            }
        } catch (Exception exc) {

        }

        return builder
                .config("spark.sql.inMemoryColumnarStorage.compressed", this.inMemoryColumnarStorageCompressed)
                .config("spark.sql.files.maxPartitionBytes", this.maxPartitionBytes)
                .config("spark.sql.shuffle.partitions", this.shufflePartitions)
                .config("spark.shuffle.compress", this.shuffleCompress)
                .config("spark.shuffle.spill.compress", this.shuffleSpillCompress)
                .config("spark.broadcast.compress", this.broadcastCompress)
                .config("spark.rdd.compress", this.rddCompress)
                .config("spark.default.parallelism", this.defaultParallelism);
        //.config("spark.executor.cores", this.executorCores)
        //.config("spark.task.cpus", this.taskCpus);
    }
}