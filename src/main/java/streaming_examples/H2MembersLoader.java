package streaming_examples;

import com.github.javafaker.Faker;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Evgeny Borisov
 */
@Service
@Transactional
public class H2MembersLoader {
    private Faker faker = new Faker();
    @Autowired
    private MembersDetailsRepo repo;

    @Autowired
    private SparkSession spark;

    @EventListener(ContextRefreshedEvent.class)
    public void fillDB(){
        for (int i = 0; i < 39; i++) {
            repo.save(MemberDetails.builder().id(i).address(faker.address().fullAddress()).build());
        }
    }

    public Dataset<Row> load(){
       return spark.read()
               .format("jdbc")
               .option("url", "jdbc:h2:mem:membersss")
               .option("dbtable", "members")
               .option("user", "sa")
               .option("driver", "org.h2.Driver")
               .load();

    }
}
