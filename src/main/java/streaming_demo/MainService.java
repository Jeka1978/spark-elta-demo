package streaming_demo;

import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
@Service
public class MainService {
    @Autowired
    private SparkSession spark;
    @Autowired
    private List<ColumnAppender> columnAppenders;

    @Autowired
    private MemberDFLoader memberDFLoader;

    @Autowired
    private H2Loader h2Loader;


    @SneakyThrows
    public void main() {
        Dataset<Row> membersStreamingDF = memberDFLoader.load();
        for (ColumnAppender columnAppender : columnAppenders) {
            membersStreamingDF = membersStreamingDF.withColumn(columnAppender.getName(), columnAppender.getColumn());
        }

        Dataset<Row> membersInfoDF = h2Loader.load();
        membersStreamingDF = membersStreamingDF.join(membersInfoDF, membersInfoDF.col("memid").equalTo(membersStreamingDF.col("memid")));
//        membersStreamingDF.writeStream().format("console").outputMode(OutputMode.Update()).start();

        membersStreamingDF = membersStreamingDF.groupBy(window(col("starttime"), "1 days", "1 seconds")).agg(avg(col("age")));

        membersStreamingDF.writeStream().format("console").outputMode(OutputMode.Update()).start();
        spark.streams().awaitAnyTermination();

    }


}
