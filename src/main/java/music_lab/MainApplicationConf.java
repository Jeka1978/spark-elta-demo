package music_lab;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * @author Evgeny Borisov
 */
@SpringBootApplication
public class MainApplicationConf {

    @Bean
    public SparkSession sparkSession(){
        return SparkSession.builder().master("local[*]").appName("music judge").getOrCreate();
    }




    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MainApplicationConf.class);
    }
}
