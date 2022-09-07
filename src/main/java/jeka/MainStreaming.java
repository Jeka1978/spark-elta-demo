package jeka;

import lombok.SneakyThrows;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;

import java.io.File;
import java.util.Iterator;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
public class MainStreaming {
    @SneakyThrows
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("streaming DEMO")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath())
                .getOrCreate();

        spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
//                .schema(Encoders.bean(Person.class).schema())
                .load()
//                .as(Encoders.bean(Person.class))
//                .groupBy(window(col("eventTime"), "10 min", "5 min"), col("address.city"))
//                .agg(max(count(col("city")).asc()))
//                .agg(count(col("address.city")).as("count"))

               /* .groupByKey()
                .mapGroupsWithState(new MapGroupsWithStateFunction<Key, Person, State, AggResult>() {
                    @Override
                    public AggResult call(Key o, Iterator<Person> iterator, GroupState<State> groupState) throws Exception {
                        groupState.
                        return null;
                    }
                }, Encoders.bean(), Encoders.bean(), GroupStateTimeout.NoTimeout())*/
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Update())
                .start()
                .awaitTermination();
    }
}
