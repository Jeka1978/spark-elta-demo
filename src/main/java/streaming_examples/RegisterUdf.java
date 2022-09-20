package streaming_examples;/**
 * @author Evgeny Borisov
 */

import org.springframework.stereotype.Component;

import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Component
@Retention(RUNTIME)
public @interface RegisterUdf {
}
