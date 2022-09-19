package pure.spark.labs;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author Evgeny Borisov
 */
@Component
public class SimpleUdf implements UDF2<String,Integer,String> {

    @Autowired
    private transient SparkSession sparkSession;

    @PostConstruct
    public void init(){
        sparkSession.udf().register(this.getClass().getName(), this, DataTypes.StringType);
    }


    @Override
    public String call(String firstName, Integer id) throws Exception {
        return firstName.toUpperCase()+id*2;
    }
}
