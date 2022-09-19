package pure.spark.labs;

import com.github.javafaker.Faker;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Evgeny Borisov
 */
@RegisterUdf
public class NickNameCalculatorUdf implements UDF1<Integer,String> {

    private Map<Integer,String> id2NickName = new HashMap<>();


    @Autowired
    protected void init(Faker faker, SparkSession spark) {
        for (int i = 0; i < 40; i++) {
            id2NickName.put(i, faker.gameOfThrones().character());
        }
    }

    @Override
    public String call(Integer memid) throws Exception {
        return id2NickName.getOrDefault(memid,"no name");
    }
}
