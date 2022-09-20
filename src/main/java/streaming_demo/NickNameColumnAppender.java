package streaming_demo;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;

/**
 * @author Evgeny Borisov
 */
@Component
public class NickNameColumnAppender implements ColumnAppender {
    @Override
    public Column getColumn() {
        return functions.lit("no name");
    }

    @Override
    public String getName() {
        return "nickName";
    }
}
