package linkedIn;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
@SpringBootApplication
public class MainApp {

    public static final String SALARY = "salary";
    public static final String KEYWORDS = "keywords";
    public static final String KEYWORD = "keyword";

    @Bean
    public SparkSession sparkSession(){
        return SparkSession.builder().appName("linked in").master("local[*]").getOrCreate();
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MainApp.class);
        SparkSession spark = context.getBean(SparkSession.class);
        Dataset<Row> dataframe = spark.read().json("data/linkedIn/profiles.json");
        dataframe.persist();
        dataframe.show();
        dataframe.printSchema();
     /*   Dataset<Row> salaryDF = dataframe.withColumn("salary", col("age").multiply(10).multiply(size(col("keywords"))));
        salaryDF.show();
        Dataset<Row> keywordDF = dataframe.withColumn(KEYWORD, explode(col(KEYWORDS))).select("keyword");
        Row row = keywordDF.groupBy(col(KEYWORD))
                .agg(count(col(KEYWORD)).as("total"))
                .orderBy(col("total").desc())
                .first();
        String mostPopular = row.getAs(KEYWORD);
        System.out.println("mostPopular = " + mostPopular);
        salaryDF.filter(array_contains(col(KEYWORDS),mostPopular))
                .filter(col(SALARY).leq(1200))
                .show();*/
        dataframe.registerTempTable("keywords");
        spark.sql("select * from keywords").show();
    }
}
