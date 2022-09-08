package music_lab;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
public interface MusicService {
    List<String> mostPopular(String path, int amount);
}
