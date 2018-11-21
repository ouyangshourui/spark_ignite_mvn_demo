package com.zhw.bigdata.ignite;

import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.ignite.spark.IgniteDataFrameSettings.*;
import org.apache.spark.sql.execution.columnar.LONG;
import scala.Long;


public class SparkIgnite4Csv {
   // private static final String CONFIG = "/Users/ouyangshourui/code/ignite/src/main/resources/client.xml";

    public static void main(String[] args) {
        if(args.length<3){
            System.out.println("SparkIgnite4Csv csvpath  igniteconfigpath tablename");
        }
        final String CONFIG = args[1];
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Ignite data sources write example")
                .master("local[4]")
                .config("spark.executor.instances", "2")
                .getOrCreate();
        spark.sparkContext().setLogLevel("warn");
        //Dataset<Row> event_10001_user = spark.read().csv("/Users/ouyangshourui/code" +
         //       "/ignite/src/main/resources/event_10001_useraa.csv");
        Dataset<Row> event_10001_user = spark.read().csv(args[0]);

        event_10001_user.show();
        event_10001_user.schema();
        event_10001_user.printSchema();
        event_10001_user.registerTempTable(args[2]);
        Dataset<Row> event_10001_user_table = spark.sql("select " +
                "cast(_c0 as Long) as _ftenancy_id," +
                "cast(_c1 as String) as _fid, " +
                "cast(_c2 as Long) as fsbabyage, " +
                "cast(_c3 as String) as fsbirthday, " +
                "cast(_c4 as Long) as fuserlevel, " +
                "cast(_c5 as Long) as fbabynum, " +
                "cast(_c6 as String) as fbbirthday, " +
                "cast(_c7 as String) as fbirthday, " +
                "cast(_c8 as String) as  fcreatordepartment, " +
                "cast(_c9 as String) as  fearliestregtime, " +
                "cast(_c10 as String) as  fmobile, " +
                "cast(_c11 as Long) as  fsex, " +
                "cast(_c12 as Long) as  fssex, " +
                "cast(_c13 as String) as  ftruename, " +
                "cast(_c14 as Long) as  fbbabyage, " +
                "cast(_c15 as Long) as  fbsex, " +
                "cast(_c16 as String) as  fcityname, " +
                "cast(_c17 as String) as  fcommunity, " +
                "cast(_c18 as String) as  fdistrictname, " +
                "cast(_c19 as String) as  fgestationage, " +
                "cast(_c20 as String) as  fnickname, " +
                "cast(_c21 as String) as  fpromoteactive, " +
                "cast(_c22 as String) as  fprovincename, " +
                "cast(_c23 as String) as  fcreator, " +
                "cast(_c24 as String) as  fbirthday_next, " +
                "cast(_c25 as Long) as  fmemberlevel " +
                " from "+ args[2]);
        event_10001_user_table.show(10);

        System.out.println("Writing Data Frame to Ignite:");

        //Writing content of data frame to Ignite.


//        Dataset<Row> event_10001_user_df=spark.createDataFrame( event_10001_user.rdd(),df.schema());
//
//        event_10001_user_df.printSchema();

        //Writing content of data frame to Ignite.
        event_10001_user_table.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), args[2])
                .option(IgniteDataFrameSettings.OPTION_STREAMER_ALLOW_OVERWRITE(),"true")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "_FTENANCY_ID,_FID")
                .mode(SaveMode.Overwrite)
                .save();




        System.out.println("write data Done");
        Dataset<Row> EVENT_10001_USER_data = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), args[2])
                .load();
        EVENT_10001_USER_data.printSchema();

        EVENT_10001_USER_data.show(100);
        System.out.println("show EVENT_10001_USER_data done!");

        System.out.println("Reading data from Ignite table:");
        spark.close();
    }
}
