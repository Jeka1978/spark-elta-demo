package streaming_demo;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Evgeny Borisov
 */
public interface MembersRepo extends JpaRepository<Member,Integer> {
}
