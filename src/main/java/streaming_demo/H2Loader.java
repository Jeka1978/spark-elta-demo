package streaming_demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Evgeny Borisov
 */
@Service
public class H2Loader {
    @Autowired
    private SparkSession spark;

    public Dataset<Row> load(){
      return spark.read()
                .format("jdbc")
                .option("url", "jdbc:h2:mem:membersss")
                .option("dbtable", "members")
                .option("user", "sa")
                .option("driver", "org.h2.Driver")
                .load();
    }
}
