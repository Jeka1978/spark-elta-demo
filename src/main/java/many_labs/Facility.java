package many_labs;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.LogicalTypes;
import org.apache.spark.sql.types.Decimal;

import java.math.BigDecimal;

/**
 * @author Evgeny Borisov
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Facility {
    private Integer facid;
    private String name;
}
