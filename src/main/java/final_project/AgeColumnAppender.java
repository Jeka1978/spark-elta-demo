package final_project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.rand;
import static org.apache.spark.sql.functions.round;

/**
 * @author Evgeny Borisov
 */
@Component
public class AgeColumnAppender implements ColumnAppender {


    @Override
    public Dataset<Row> append(Dataset<Row> df) {
        return df.withColumn("age", round(rand().multiply(150)));
    }
}
