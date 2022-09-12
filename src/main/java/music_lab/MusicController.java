package music_lab;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;

/**
 * @author Evgeny Borisov
 */
@RestController
@RequestMapping("/api/")
public class MusicController {

    @Autowired
    private SparkSession spark;

    @GetMapping("count")
    public long calculateWordsNumber(@RequestParam String path) {
        Dataset<String> dataset = spark.read().textFile(path);
        dataset.show();
        return dataset.count();
    }
}
