package many_labs;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDate;

/**
 * @author Evgeny Borisov
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Member {
    private Integer memid;
    private String surname;
    private String firstname;
    private String address;
    private Integer zipcode;
    private String telephone;
    private Integer recommendedby;
    private Instant joindate;


}
