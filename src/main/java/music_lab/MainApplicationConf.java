package music_lab;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import scala.reflect.ClassTag;

/**
 * @author Evgeny Borisov
 */
@SpringBootApplication
@PropertySource("classpath:user.properties")
public class MainApplicationConf {


    @Bean
    public Broadcast<UserProps> userPropsBroadcast(UserProps userProps) {
       return sparkSession().sparkContext().broadcast(userProps, ClassTag.apply(UserProps.class));
    }

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder().master("local[*]").appName("music judge").getOrCreate();
    }


    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MainApplicationConf.class);
    }
}
