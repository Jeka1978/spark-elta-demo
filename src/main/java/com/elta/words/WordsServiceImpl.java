package com.elta.words;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
public class WordsServiceImpl implements WordsService {
    @Override
    public List<String> topX(JavaRDD<String> lines, int x) {
        return null;
    }
}
