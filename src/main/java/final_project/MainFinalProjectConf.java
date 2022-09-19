package final_project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * @author Evgeny Borisov
 */
@SpringBootApplication
public class MainFinalProjectConf {

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder().appName("csv").master("local[*]").getOrCreate();
    }



    @Bean
    public Dataset<Row> membersDf() {
        StructType structTypeMember = new StructType()
                .add("memid", DataTypes.IntegerType)
                .add("surname", DataTypes.StringType)
                .add("firstname", DataTypes.StringType)
                .add("address", DataTypes.StringType)
                .add("zipcode", DataTypes.IntegerType)
                .add("telephone", DataTypes.StringType)
                .add("recommendedby", DataTypes.IntegerType)
                .add("joindate", DataTypes.TimestampType);
        return sparkSession().read().option("header", "true").schema(structTypeMember).csv("data/members.csv").persist();
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MainFinalProjectConf.class);
        context.getBean(MainService.class).flow();
    }

}
