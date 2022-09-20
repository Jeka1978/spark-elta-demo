package streaming_demo;

import org.apache.spark.sql.Column;

/**
 * @author Evgeny Borisov
 */
public interface ColumnAppender {
    Column getColumn();
    String getName();
}
