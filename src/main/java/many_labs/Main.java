package many_labs;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("labs").master("local[*]").getOrCreate();

        StructType structTypeMemeber = new StructType()
                .add("memid", DataTypes.IntegerType)
                .add("surname", DataTypes.StringType)
                .add("firstname", DataTypes.StringType)
                .add("address", DataTypes.StringType)
                .add("zipcode", DataTypes.IntegerType)
                .add("telephone", DataTypes.StringType)
                .add("recommendedby", DataTypes.IntegerType)
                .add("joindate", DataTypes.TimestampType);

        StructType structTypeFacility = new StructType()
                .add("facid", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("membercost",DataTypes.createDecimalType(38, 18))
                .add("guestcost",DataTypes.createDecimalType(38, 18))
                .add("initialoutlay",DataTypes.createDecimalType(38, 18))
                .add("monthlymaintenance", DataTypes.createDecimalType(38, 18));

//        spark.read().option("header","true").schema(structType).csv("data/members.csv").show();
      /*  spark.read().option("header", "true").schema(Encoders.bean(Facility.class).schema())
                .csv("data/facilities.csv")
                .filter(lower(col("name")).contains("tennis")).show();*/
    /*    spark.read().option("header", "true").schema(structTypeFacility)
                .csv("data/facilities.csv")
                .withColumn("price",when(col("monthlymaintenancemonthlymaintenance").leq(30),"cheap").otherwise("expensive")).show();*/

     /*
        Dataset<Row> dataset = spark.read().option("header", "true").csv("data/members.csv")
                .select("memid","surname","firstname","address","telephone");
        RDD<Row> rdd = dataset.rdd();
        spark.sqlContext().createDataFrame(rdd,Encoders.bean(Member.class).schema()).show();*/

   /*     spark.read().option("header","true").csv("data/members.csv")
                .select(col("memid").cast(DataTypes.IntegerType),to_timestamp(col("joindate")).as("joindate"))
                .as((Encoders.bean(Member.class)))
                .show(); */
        Dataset<Row> rowDataset = spark.read().option("header", "true")
                .schema(structTypeFacility)
                .csv("data/facilities.csv");

        rowDataset.filter(col("facid").equalTo(1).or(col("facid").equalTo(5))).show();

//        rowDataset.agg(max(col("joindate"))).show();
//        rowDataset.orderBy(col("joindate").desc()).limit(1).show();
//        Dataset<String> dataset = rowDataset.select("surname").as(Encoders.STRING());
        Row row = rowDataset.select("surname").reduce((ReduceFunction<Row>) (row1, row2) -> RowFactory.create((String)row1.get(0)));
//        System.out.println("answer:: "+row.get(0));
//        System.out.println("surname " + row.getAs("surname").toString());

//        rowDataset.select(lower(col("surname"))).distinct().orderBy(col("surname").desc()).takeAsList(10).forEach(System.out::println);

//        rowDataset.filter(col("joindate").gt("2012-09-01")).show();

//        rowDataset.map((MapFunction<Row, Member>) row -> new Member(),Encoders.bean(Member.class))


    }
}
