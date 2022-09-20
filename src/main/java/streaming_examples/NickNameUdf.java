package streaming_examples;

import com.github.javafaker.Faker;
import com.github.javafaker.GameOfThrones;
import org.apache.spark.sql.api.java.UDF0;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Evgeny Borisov
 */
@RegisterUdf
public class NickNameUdf implements UDF0<String> {

    private transient GameOfThrones faker = new Faker().gameOfThrones();
    private List<String> nickNames;

    @PostConstruct
    public void init(){
        nickNames = Stream.generate(faker::character).limit(40).collect(Collectors.toList());
    }


    @Override
    public String call() throws Exception {
        return nickNames.remove(0);
    }
}
