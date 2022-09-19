package pure.spark.labs;/**
 * @author Evgeny Borisov
 */

import org.apache.spark.sql.types.DataType;
import org.springframework.stereotype.Component;

import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Component
@Retention(RUNTIME)
public @interface RegisterUdf {
}
