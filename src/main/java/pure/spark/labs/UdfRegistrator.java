package pure.spark.labs;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collection;

/**
 * @author Evgeny Borisov
 */
@Component
public class UdfRegistrator {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private SparkSession spark;

    @EventListener(ContextRefreshedEvent.class)
    public void registerUdf(){
        Collection<Object> udfs = context.getBeansWithAnnotation(RegisterUdf.class).values();
        for (Object udf : udfs) {
            if (udf instanceof UDF1) {
                spark.udf().register(udf.getClass().getName(), (UDF1<?,?>) udf, DataTypes.StringType);
            }
        }
    }
}
