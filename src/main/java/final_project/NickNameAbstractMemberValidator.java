package final_project;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;

/**
 * @author Evgeny Borisov
 */
@Component
public class NickNameAbstractMemberValidator extends AbstractMemberValidator {
    @Override
    protected String getReasonWhyDataNotValid() {
        return "nickName is too long";
    }

    @Override
    protected Column getValidData() {
        return functions.length(functions.col("nickName")).leq(5);
    }
}
