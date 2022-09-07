package jeka;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * @author Evgeny Borisov
 */
@SpringBootApplication
public class SpringBootMain {

    @Bean
    public SparkSession sparkSession() {
        SparkSession sparkSession = SparkSession.builder().appName("a").master("local[*]").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        return sparkSession;
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBootMain.class);
    }
}
