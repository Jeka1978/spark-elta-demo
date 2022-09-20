package streaming_examples;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
@Component
public class BookingColumnAppenderForAge implements BookingColumnAppender {
    @Override
    public String columnName() {
        return "age";
    }

    @Override
    public Column getColumn() {
        return round(rand().multiply(150));
    }
}
