package streaming_demo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * @author Evgeny Borisov
 */
@SpringBootApplication
public class MembersApplication {

    {
        var logger = LoggerFactory.getLogger("root");
        ((Logger) logger).setLevel(Level.ERROR);
    }

    @Bean
    public SparkSession spark() {
        return SparkSession.builder().appName("streaming demo").master("local[*]").getOrCreate();

    }

    @SneakyThrows
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MembersApplication.class);
        context.getBean(MainService.class).main();
    }
}
