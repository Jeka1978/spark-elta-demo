package pure.spark.labs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

/**
 * @author Evgeny Borisov
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class UdfTest {

    @Autowired
    Dataset<Row> membersDf;



    @Test
    public void testMyUdf() {
        membersDf.show();
        membersDf.withColumn("strange col", callUDF(SimpleUdf.class.getName(), col("firstname"),col("memid"))).select("memid","firstname","strange col").show();
    }
}
