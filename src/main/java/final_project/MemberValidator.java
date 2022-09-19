package final_project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Evgeny Borisov
 */
public interface MemberValidator {
    Dataset<Row> validate(Dataset<Row> df);
}
