package streaming_demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Random;

/**
 * @author Evgeny Borisov
 */
@Service
@Transactional
public class MemberService {
    @Autowired
    private MembersRepo repo;

    @EventListener(ContextRefreshedEvent.class)
    public void initDB() {
        Random random = new Random();
        for (int i = 0; i < 40; i++) {
            repo.save(Member.builder().age(random.nextInt(150)).memid(i).build());
        }
    }
}
