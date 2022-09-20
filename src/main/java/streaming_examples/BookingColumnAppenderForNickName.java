package streaming_examples;

import org.apache.spark.sql.Column;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
@Component
public class BookingColumnAppenderForNickName implements BookingColumnAppender {
    @Override
    public String columnName() {
        return "nickName";
    }

    @Override
    public Column getColumn() {
        return callUDF(NickNameUdf.class.getName());
    }
}
