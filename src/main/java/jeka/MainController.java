package jeka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.apache.spark.sql.functions.col;

/**
 * @author Evgeny Borisov
 */
@RestController
public class MainController {
    @Autowired
    private SparkSession sparkSession;

    @GetMapping("/api/data")
    public String getData(){
        Dataset<Row> dataset = sparkSession.read().schema(Encoders.bean(Person.class).schema()).json("data/people.json");
        dataset.show();
        dataset.printSchema();

        dataset.select("*","address.*").where(col("address.state").equalTo("Ohio")).drop("address").show();

        return "ok";
    }
}
