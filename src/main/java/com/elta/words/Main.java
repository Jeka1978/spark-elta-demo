package com.elta.words;

import com.elta.words.services.MusicService;
import com.elta.words.services.WordsService;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(SparkConfig.class);
        JavaSparkContext sc = context.getBean(JavaSparkContext.class);

        MusicService musicService = context.getBean(MusicService.class);

        List<String> list = musicService.topX("data/songs/beatles/yerstaday.txt", 3);
        System.out.println(list);
    }
}
