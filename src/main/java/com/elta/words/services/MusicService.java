package com.elta.words.services;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.List;

/**
 * @author Evgeny Borisov
 */
@Service
public class MusicService {

    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private WordsService wordsService;

    public List<String> topX(String artistName, int x) {
//        JavaRDD<String> rdd = sc.textFile("data/songs/" + artistName+"/*");
        JavaRDD<String> rdd = sc.textFile(artistName);
        return wordsService.topX(rdd, x);
    }
}





