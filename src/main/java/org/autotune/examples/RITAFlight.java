package org.autotune.examples;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.autotune.AutoTune;
import org.autotune.AutoTuneDefault;
import org.autotune.config.SparkTuneableConfig;
import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

/**
 * Created by KevinRoj on 26.04.17.
 */

public class RITAFlight {

    public static void main(String [] args) throws InterruptedException {
        RITAFlight obj = new RITAFlight();
        obj.firstTest();
    }

    static private SparkSession.Builder coreSparkBuilder(int iteration) {
        return SparkSession
                .builder()
                .appName("RITA flight AutoTune " + iteration);
    }


    static private void reduceLogLevel(SparkSession spark){
        JavaSparkContext sparkContextD = JavaSparkContext.fromSparkContext(spark.sparkContext());
        sparkContextD.setLogLevel("ERROR");
    }

    static public void firstTest() throws InterruptedException {
        AutoTune<SparkTuneableConfig> tuner = new AutoTuneDefault(new SparkTuneableConfig());

        for (int t = 0; t < 3; t++) {
            {
                //warm up
                SparkSession sparkD = coreSparkBuilder(-1).getOrCreate();
                reduceLogLevel(sparkD);

                simpleSparkMethod(sparkD);
                sparkD.stop();
            }
        }

        for (int t = 0; t < 140; t++) { //40 benchmark tests
            SparkTuneableConfig cfg = tuner.start().getConfig();
            SparkSession spark = cfg.setConfig(coreSparkBuilder(t)).getOrCreate();
            reduceLogLevel(spark);

            tuner.startTimeMeasure();

            simpleSparkMethod(spark);

            tuner.stopTimeMeasure();

            spark.stop();

            tuner.end();
        }

        {
            //last time before example evaluation
            SparkSession sparkD = coreSparkBuilder(-2).getOrCreate();
            reduceLogLevel(sparkD);
            long startTimeStamp = System.currentTimeMillis();
            simpleSparkMethod(sparkD);
            System.out.println("Default cost: " + (System.currentTimeMillis() - startTimeStamp));

            sparkD.stop();
        }

        {

            System.out.println("Best configuration with result:" + tuner.getBestResult());

        }

    }

    static void simpleSparkMethod(SparkSession spark) {
        Dataset<Row> dataset1 = spark.read().format("csv").option("header", true).option("inferSchema", false).load("datasets/rita_flight/rita_flight_2008.csv");
        Dataset<Row> dataset2 = spark.read().format("csv").option("header", true).option("inferSchema", false).load("datasets/rita_flight/rita_flight_2007.csv");
        Dataset<Row> dataset3 = spark.read().format("csv").option("header", true).option("inferSchema", false).load("datasets/rita_flight/rita_flight_2006.csv");
        dataset1 = dataset1.union(dataset2).union(dataset3);

        Dataset<Row> arrivalDelay = dataset1.select("Dest", "ArrDelay")
                //safe parsing of attribute "ArrDelay"
                .map(value -> {
                    int arrDelay;
                    try {
                        arrDelay = Integer.parseInt(value.getString(1));
                    } catch (NumberFormatException e) {
                        arrDelay = 0;
                    }
                    return new Tuple2<>(value.getString(0), arrDelay);
                }, Encoders.tuple(Encoders.STRING(), Encoders.INT()))
                .toDF("Dest", "ArrDelay")
                .groupBy("Dest")
                .agg(mean("ArrDelay").alias("meanDelay"), max("ArrDelay").alias("maxDelay"), sum("ArrDelay").alias("sumDelay"))
                .sort(desc("meanDelay"), desc("maxDelay"))
                .cache();
        arrivalDelay.show();
    }
}