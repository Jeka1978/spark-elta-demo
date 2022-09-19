package final_project;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;

/**
 * @author Evgeny Borisov
 */
@Component
public class AgeMemberValidator extends AbstractMemberValidator {

    @Override
    protected String getReasonWhyDataNotValid() {
        return "age not valid";
    }

    @Override
    protected Column getValidData() {
        return functions.col("age").leq(120).and(functions.col("age").gt(0));
    }
}
