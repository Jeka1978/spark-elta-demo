package com.elta.words;

import com.elta.words.services.WordsService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
@RunWith(SpringRunner.class)
//@ActiveProfiles("DEV")
@ContextConfiguration(classes = SparkConfig.class)
public class WordsServiceImplTest {

    @Autowired
    private WordsService wordsService;

    @Autowired
    private JavaSparkContext sc;


    @Test
    public void topX() {

        JavaRDD<String> rdd = sc.parallelize(List.of("scala scala", "java java java", "Java Scala", "python"));

        List<String> list = wordsService.topX(rdd, 1);

        Assert.assertEquals(1,list.size());
        Assert.assertEquals("java",list.get(0));




    }
}