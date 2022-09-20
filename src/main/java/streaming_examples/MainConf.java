package streaming_examples;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

/**
 * @author Evgeny Borisov
 */
@SpringBootApplication
public class MainConf {

    @Bean
    public SparkSession spark(){
        var logger = LoggerFactory.getLogger("root");
        ((Logger) logger).setLevel(Level.ERROR);
        return SparkSession.builder().master("local[*]").appName("streaming").getOrCreate();
    }

    @SneakyThrows
    public static void main(String[] args) {


        SpringApplication.run(MainConf.class).getBean(EtlStreamingService.class).main();

    }
}
