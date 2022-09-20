package streaming_examples;

import org.apache.spark.sql.Column;

/**
 * @author Evgeny Borisov
 */
public interface BookingColumnAppender {
    String columnName();
    Column getColumn();
}
