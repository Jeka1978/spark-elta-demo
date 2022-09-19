package pure.spark.labs;

import com.github.javafaker.Faker;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Evgeny Borisov
 */
@Configuration
public class FakerConf {
    @Bean
    public Faker faker(){
        return new Faker();
    }
}
