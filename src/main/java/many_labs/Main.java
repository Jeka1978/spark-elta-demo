package many_labs;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("labs").master("local[*]").getOrCreate();

        StructType structType = new StructType().add("surname", DataTypes.StringType).add("firstname", DataTypes.StringType).add("address", DataTypes.StringType).add("zipcode", DataTypes.IntegerType).add("telephone", DataTypes.StringType);
        StructType structTypeFacility = new StructType().add("facid", DataTypes.IntegerType).add("name", DataTypes.StringType).add("monthlymaintenance",DataTypes.createDecimalType(38,18));

//        spark.read().option("header","true").schema(structType).csv("data/members.csv").show();
      /*  spark.read().option("header", "true").schema(Encoders.bean(Facility.class).schema())
                .csv("data/facilities.csv")
                .filter(lower(col("name")).contains("tennis")).show();*/
    /*    spark.read().option("header", "true").schema(structTypeFacility)
                .csv("data/facilities.csv")
                .withColumn("price",when(col("monthlymaintenance").leq(30),"cheap").otherwise("expensive")).show();*/

     /*
        Dataset<Row> dataset = spark.read().option("header", "true").csv("data/members.csv")
                .select("memid","surname","firstname","address","telephone");
        RDD<Row> rdd = dataset.rdd();
        spark.sqlContext().createDataFrame(rdd,Encoders.bean(Member.class).schema()).show();*/

   /*     spark.read().option("header","true").csv("data/members.csv")
                .select(col("memid").cast(DataTypes.IntegerType),to_timestamp(col("joindate")).as("joindate"))
                .as((Encoders.bean(Member.class)))
                .show(); */
        spark.read().option("header","true")
                .schema(Encoders.bean(Member.class).schema())
                .csv("data/members.csv")


                .show();

    }
}
