package final_project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;

/**
 * @author Evgeny Borisov
 */
@Component
public class NickNameColumnAppender implements ColumnAppender {
    @Override
    public Dataset<Row> append(Dataset<Row> df) {
        return df.withColumn("nickName", functions.lit("no name"));
    }
}
