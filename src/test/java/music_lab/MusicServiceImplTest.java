package music_lab;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Evgeny Borisov
 */
@SpringBootTest(classes = MainApplicationConf.class)
@RunWith(SpringRunner.class)
public class MusicServiceImplTest {

    @Autowired
    private MusicService musicService;

    @Test
    public void mostPopular() {
        List<String> list = musicService.mostPopular("data/songs/pinkfloyd", 5);
        System.out.println(list);
    }
}