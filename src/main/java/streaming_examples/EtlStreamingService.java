package streaming_examples;

import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
@Service
public class EtlStreamingService {
    @Autowired
    private BookingLoader bookingLoader;

    @Autowired
    private H2MembersLoader h2MembersLoader;

    @Autowired
    private List<BookingColumnAppender> columnAppenders;

    @SneakyThrows
    public void main() {
        Dataset<Row> bookingDF = bookingLoader.load();
        for (BookingColumnAppender columnAppender : columnAppenders) {
            bookingDF = bookingDF.withColumn(columnAppender.columnName(), columnAppender.getColumn());
        }

        Dataset<Row> membersDetailsDF = h2MembersLoader.load();

        bookingDF = bookingDF.join(membersDetailsDF, bookingDF.col("memid").equalTo(membersDetailsDF.col("id")));


        bookingDF.writeStream().format("console").outputMode(OutputMode.Update()).start().awaitTermination();


    }
}
