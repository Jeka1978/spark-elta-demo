package final_project;

import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/**
 * @author Evgeny Borisov
 */
@Component
@Aspect
public class DataFrameLoggerAspect {

    @Around("@annotation(ShowDf)")
    @SneakyThrows
    public Object showDF(ProceedingJoinPoint pjp) {

        Object[] args = pjp.getArgs();
        if (args[0] instanceof Dataset) {
            System.out.println("showing dataframe before");
            Class<?> clazz = pjp.getTarget().getClass();
            System.out.println("clazz = " + clazz);
            ((Dataset<?>) args[0]).show();
        }
        Object retVal = pjp.proceed();
        if (retVal instanceof Dataset) {
            System.out.println("************** end ***********");
            ((Dataset<?>) retVal).show();
            System.out.println("************** end ***********");
        }
        return retVal;
    }
}



