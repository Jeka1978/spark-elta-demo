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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

/**
 * @author Evgeny Borisov
 */
public class Main {
    @SneakyThrows
    public static void main(String[] args) {
        var logger = LoggerFactory.getLogger("root");
        ((Logger) logger).setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().master("local[*]").appName("streaming").getOrCreate();
        StructType structTypeBookings = new StructType()
                .add("bookid", DataTypes.IntegerType)
                .add("facid", DataTypes.IntegerType)
                .add("memid", DataTypes.IntegerType)
                .add("starttime", DataTypes.TimestampType)
                .add("slots", DataTypes.IntegerType);
        Dataset<Row> bookingDf = spark.readStream()
                .option("header", "true")
                .schema(structTypeBookings)
                .csv("streaming_data/");


        bookingDf = bookingDf.withColumn("cost", col("facid").multiply(10)).groupBy(col("memid")).agg(sum(col("cost")));

        bookingDf.writeStream().format("console").outputMode(OutputMode.Update()).start();



        spark.streams().awaitAnyTermination();

    }
}
