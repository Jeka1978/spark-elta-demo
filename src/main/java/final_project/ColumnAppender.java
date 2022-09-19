package final_project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Evgeny Borisov
 */
public interface ColumnAppender {
    Dataset<Row> append(Dataset<Row> df);
}
