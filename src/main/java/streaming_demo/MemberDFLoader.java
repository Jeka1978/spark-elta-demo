package streaming_demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Evgeny Borisov
 */
@Service
public class MemberDFLoader {
    @Autowired
    private SparkSession spark;

    public Dataset<Row> load() {
        StructType structTypeBookings = new StructType()
                .add("bookid", DataTypes.IntegerType)
                .add("facid", DataTypes.IntegerType)
                .add("memid", DataTypes.IntegerType)
                .add("starttime", DataTypes.TimestampType)
                .add("slots", DataTypes.IntegerType);
        return spark.readStream().option("header", "true").schema(structTypeBookings).csv("streaming_data/");

    }
}
