package pure.spark.labs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
//        StructField age = new StructField("age", DataTypes.IntegerType, false, Metadata.empty());
//        StructType structType = new StructType(new StructField[]{age, name,});
        StructType schema = new StructType().add("name", DataTypes.StringType).add("age", DataTypes.IntegerType);

        SparkSession spark = SparkSession.builder().appName("pure spark labs").master("local[*]").getOrCreate();
        Dataset<Row> csv = spark.read().option("header", "true").schema(schema).csv("data/members.csv");
        Dataset<Row> rowDataset = csv.withColumn("age", when(col("birtday").gt("2000/1/1"), "adult").otherwise("young"));
        Dataset<Row> age = rowDataset.agg(functions.max(col("joindate")));
    }
}
