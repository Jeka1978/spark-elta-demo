package final_project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
@Service
public class MemberMainValidator {
    @Autowired
    private List<MemberValidator> validatorList;

    @ShowDf
    public Dataset<Row> validate(Dataset<Row> memberDF) {
        for (MemberValidator memberValidator : validatorList) {
            memberDF=memberValidator.validate(memberDF);
        }
        return memberDF;
    }
}
