package final_project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
@Service
public class MemberColumnAppender {

    @Autowired
    private List<ColumnAppender> appenders;

    public Dataset<Row> appendAll(Dataset<Row> df) {

        for (ColumnAppender appender : appenders) {
            df = appender.append(df);
        }
        return df;
    }
}
