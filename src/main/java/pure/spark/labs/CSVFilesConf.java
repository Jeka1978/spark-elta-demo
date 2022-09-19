package pure.spark.labs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;

/**
 * @author Evgeny Borisov
 */
@SpringBootApplication
public class CSVFilesConf {

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

    @Bean
    public Dataset<Row> facilitiesDf() {
        StructType structTypeFacility = new StructType()
                .add("facid", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("membercost",DataTypes.createDecimalType(38,18))
                .add("guestcost",DataTypes.createDecimalType(38,18))
                .add("initialoutlay",DataTypes.createDecimalType(38,18))
                .add("monthlymaintenance",DataTypes.createDecimalType(38,18));
        return sparkSession().read().option("header", "true").schema(structTypeFacility).csv("data/facilities.csv").persist();
    }

    @Bean
    public Dataset<Row> bookingsDf() {
        StructType structTypeBookings = new StructType()
                .add("bookid", DataTypes.IntegerType)
                .add("facid", DataTypes.IntegerType)
                .add("memid", DataTypes.IntegerType)
                .add("starttime", DataTypes.TimestampType)
                .add("slots", DataTypes.IntegerType);
        return sparkSession().read().option("header", "true").schema(structTypeBookings).csv("data/bookings.csv");
    }
}
