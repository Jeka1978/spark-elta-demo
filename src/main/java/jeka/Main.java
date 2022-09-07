package jeka;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;


/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("a").master("local[*]").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        Dataset<Row> dataset = sparkSession.read().schema(Encoders.bean(Person.class).schema()).json("data/people.json");
        dataset.show();
        dataset.printSchema();

        dataset.select("*","address.*").where(col("address.state").equalTo("Ohio")).drop("address").show();
    }
}
