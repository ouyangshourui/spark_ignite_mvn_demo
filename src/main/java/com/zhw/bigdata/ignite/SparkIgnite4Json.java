package com.zhw.bigdata.ignite;

import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

/**
 * 240.119
 * spark-submit --master local[4] --executor-memory 16g --driver-memory 16g --class com.zhw.bigdata.ignite.SparkIgnite4Csv ignite-1.0-SNAPSHOT-jar-with-dependencies.jar /home/ourui/event_10001_user.csv /home/ourui/client.xml event_10001_userp
 */

public class SparkIgnite4Json {
    private static final String CONFIG = "/Users/ouyangshourui/code/ignite/src/main/resources/client.xml";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Ignite data sources write example")
                .master("local")
                .config("spark.executor.instances", "2")
                .getOrCreate();
        Dataset<Row> personsDataFrame = spark.read().json("/Users/ouyangshourui/code" +
                "/ignite/src/main/resources/person.json");

        personsDataFrame.show();

        System.out.println("Writing Data Frame to Ignite:");

        //Writing content of data frame to Ignite.
        /**
        personsDataFrame.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "json_person")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "template=replicated")
                .mode(SaveMode.Overwrite)
                .save();
         **/
        //Writing content of data frame to Ignite.
        personsDataFrame.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "json_person")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
                .mode(SaveMode.Overwrite)
                .save();
        System.out.println("Done!");

        System.out.println("Reading data from Ignite table:");
    }
}
