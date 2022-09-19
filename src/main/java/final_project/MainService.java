package final_project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Evgeny Borisov
 */
@Service
public class MainService {

    @Autowired
    private MemberLoader memberLoader;

    @Autowired
    private MemberColumnAppender appender;

    @Autowired
    private MemberMainValidator validator;

    public void flow(){
        Dataset<Row> memberDF = memberLoader.load();
        memberDF = appender.appendAll(memberDF);
        memberDF = validator.validate(memberDF);
        memberDF.show();
    }
}
