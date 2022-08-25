package com.elta;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
public class MainSpark {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("stam");
        sparkConf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> rdd = sc.parallelize(List.of(1, 2, 3));

        List<Integer> list = rdd.filter(i -> i < 3).collect();
        list.forEach(System.out::println);
    }
}
