package streaming_examples;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Evgeny Borisov
 */
public interface MembersDetailsRepo extends JpaRepository<MemberDetails,Integer> {
}
