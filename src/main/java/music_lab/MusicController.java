package music_lab;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.File;

/**
 * @author Evgeny Borisov
 */
@RestController
@RequestMapping("/api/")
public class MusicController {

    @Autowired
    private SparkSession spark;

    @Autowired
    private MusicService musicService;

    @GetMapping("judge/{artist1}/{artist2}/{amount}")
    public double judge(@PathVariable String artist1, @PathVariable String artist2, @PathVariable int amount){
        return musicService.judgeArtists("data/songs/" + artist1, "data/songs/" + artist2, amount);

    }

    @GetMapping("count")
    public long calculateWordsNumber(@RequestParam String path) {
        Dataset<String> dataset = spark.read().textFile(path);
        dataset.show();
        return dataset.count();
    }
}
