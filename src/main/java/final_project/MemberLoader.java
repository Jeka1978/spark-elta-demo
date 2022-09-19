package final_project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Evgeny Borisov
 */
@Service
public class MemberLoader {

    @Autowired
    private Dataset<Row> memberDf;

    public Dataset<Row> load() {
        return memberDf;
    }
}
