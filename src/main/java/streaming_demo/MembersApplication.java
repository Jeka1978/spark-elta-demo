package streaming_demo;

import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * @author Evgeny Borisov
 */
public class MembersApplication {

    @SneakyThrows
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("streaming demo").master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        StructType structTypeMember = new StructType()
                .add("memid", DataTypes.IntegerType)
                .add("surname", DataTypes.StringType)
                .add("firstname", DataTypes.StringType)
                .add("address", DataTypes.StringType)
                .add("zipcode", DataTypes.IntegerType)
                .add("telephone", DataTypes.StringType)
                .add("recommendedby", DataTypes.IntegerType)
                .add("joindate", DataTypes.TimestampType);
        Dataset<Row> membersStreamingDF = spark.readStream().option("header", "true").schema(structTypeMember).csv("streaming_data/");

        membersStreamingDF.writeStream().format("console").outputMode(OutputMode.Update()).start().awaitTermination();
    }
}
