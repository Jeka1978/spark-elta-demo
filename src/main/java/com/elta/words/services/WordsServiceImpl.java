package com.elta.words.services;

import com.elta.words.props.UserProperties;
import com.elta.words.utils.WordsUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.List;

/**
 * @author Evgeny Borisov
 */
@Service
public class WordsServiceImpl implements WordsService, Serializable {


    @Autowired
    private Broadcast<UserProperties> userProps;


    @Override
    public List<String> topX(JavaRDD<String> lines, int x) {

        return lines.flatMap(WordsUtil::getWords)
                .map(String::toLowerCase)
                .filter(word -> !this.userProps.getValue().getGarbage().contains(word))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .map(Tuple2::_2)
                .take(x);

    }
}
