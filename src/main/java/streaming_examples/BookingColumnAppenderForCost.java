package streaming_examples;

import org.apache.spark.sql.Column;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

/**
 * @author Evgeny Borisov
 */
@Component
public class BookingColumnAppenderForCost implements BookingColumnAppender {
    @Override
    public String columnName() {
        return "cost";
    }

    @Override
    public Column getColumn() {
        return col("facid").multiply(10);
    }
}
