package music_lab;

import com.elta.words.utils.WordsUtil;
import lombok.SneakyThrows;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.sparkproject.guava.cache.Cache;
import org.sparkproject.guava.cache.CacheBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
@Service
public class MusicServiceImpl implements MusicService, Serializable {

    @Autowired
    private SparkSession spark;

    @Autowired
    private Broadcast<UserProps> userProps;


    Cache<String, Dataset<Row>> cache = CacheBuilder.newBuilder().concurrencyLevel(1).weakKeys().softValues().expireAfterAccess(10, TimeUnit.MINUTES).build();


    @SneakyThrows
    @Override
    public List<String> mostPopular(String path, int amount) {

        Dataset<Row> artistDF = cache.get(path, () -> Files.list(Paths.get(path))
                .map(fileName -> getDataFrame(fileName.toString()))
                .reduce(Dataset::union)
                .get()
                .groupBy(col("word")).agg(count(col("word")).as("amount"))
                .orderBy(col("amount").desc())
                .persist());
        artistDF.show(amount);
        return artistDF.takeAsList(amount).stream().map(row -> (String) row.getAs("word")).collect(Collectors.toList());


    }

    @Override
    public double judgeArtists(String artist1, String artist2, int amount) {
        Set<String> set1 = new HashSet<>(mostPopular(artist1, amount));
        Set<String> set2 = new HashSet<>(mostPopular(artist2, amount));
        double common = set1.stream().filter(set2::contains).count();
        return common/amount*100;
    }

    private Dataset<Row> getDataFrame(String fileName) {
        Dataset<String> dataset = spark.read().textFile(fileName);
        Dataset<String> wordsDF = dataset.flatMap((FlatMapFunction<String, String>) WordsUtil::getWords, Encoders.STRING());
        return wordsDF
                .select(lower(col("value")).as("word"))
                .where(not(col("word").isin(userProps.value().getGarbage().toArray())));
//        return wordsDF.withColumn("word", lower(col("value"))).drop("value").filter(not(col("word").isin(userProps.value().getGarbage().toArray())));


//        dataset.withColumn("words", split(col("value"),"\\W+")).drop("value").withColumn("word",explode(col("words"))).drop("words").show();


    }
}
