package linkedIn;

import jeka.Person;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("linked in").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        Dataset<Row> dataset = sparkSession.read().json("data/linkedIn/profiles.json");
        dataset.show();
    }
}
