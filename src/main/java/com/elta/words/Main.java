package com.elta.words;

import com.elta.words.services.MusicService;
import com.elta.words.services.WordsService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().appName("a").master("local[*]").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        System.out.println(sparkSession.read().textFile("data/songs/beatles/yerstaday.txt").count());
        /*AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(SparkConfig.class);
        JavaSparkContext sc = context.getBean(JavaSparkContext.class);

        MusicService musicService = context.getBean(MusicService.class);

        List<String> list = musicService.topX("data/songs/beatles/yerstaday.txt", 3);
        System.out.println(list);*/
    }
}
