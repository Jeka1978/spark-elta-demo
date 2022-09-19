package final_project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

/**
 * @author Evgeny Borisov
 */
@Component
public class MemberExceptionHandlerImpl implements MemberExceptionHandler {
    @Override
    public void handle(Dataset<Row> notValidData, String message) {
        System.out.println("**************** NOT VALID DATA *************************");
        System.out.println(message);
        notValidData.show();
        System.out.println("**************** NOT VALID DATA END*************************");
    }
}
