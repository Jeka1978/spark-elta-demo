package final_project;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;

import static org.apache.spark.sql.functions.not;

/**
 * @author Evgeny Borisov
 */
public abstract class AbstractMemberValidator implements MemberValidator {
    @Autowired
    private MemberExceptionHandler memberExceptionHandler;
    @Override
    public final Dataset<Row> validate(Dataset<Row> df) {
        df.persist();
        df.count();
        Dataset<Row> notValidData = df.filter(not(getValidData()));
        memberExceptionHandler.handle(notValidData, getReasonWhyDataNotValid());
        return df.filter(getValidData());
    }

    protected abstract String getReasonWhyDataNotValid();

    protected abstract Column getValidData();
}
