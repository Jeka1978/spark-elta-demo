package com.elta.words;

import com.elta.words.props.UserProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * @author Evgeny Borisov
 */
@Configuration
@ComponentScan
@PropertySource("classpath:user.properties")
public class SparkConfig {


    @Bean
    public Broadcast<UserProperties> userPropertiesBroadcast(UserProperties userProperties) {
        return sc().broadcast(userProperties);
    }

    @Bean
//    @Profile("DEV")
    public JavaSparkContext sc() {
        SparkConf conf = new SparkConf().setAppName("songs").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        return sc;
    }

    /*@Bean
    @Profile("PROD")
    public JavaSparkContext sc2(){
        SparkConf conf = new SparkConf().setAppName("songs").setMaster("local[*]");
        return new JavaSparkContext(conf);
    }*/

}
