package music_lab;

import com.elta.words.utils.WordsUtil;
import lombok.SneakyThrows;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
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

    @SneakyThrows
    @Override
    public List<String> mostPopular(String path, int amount) {

        Dataset<Row> allWordsDF = Files.list(Paths.get(path))
                .map(fileName -> getDataFrame(fileName.toString()))
                .reduce(Dataset::union)
                .get();

        Dataset<Row> rowDataset = allWordsDF.groupBy(col("word")).agg(count(col("word")).as("amount"))
                .orderBy(col("amount").desc());

        rowDataset.show();

        return rowDataset.takeAsList(amount).stream().map(row -> (String) row.getAs("word")).collect(Collectors.toList());


    }

    private Dataset<Row> getDataFrame(String fileName) {
        Dataset<String> dataset = spark.read().textFile(fileName);
        Dataset<Row> worfdsDF = dataset.flatMap((FlatMapFunction<String, String>) WordsUtil::getWords, Encoders.STRING()).select("value");
        return worfdsDF.withColumn("word", lower(col("value"))).drop("value").filter(not(col("word").isin(userProps.value().getGarbage().toArray())));


//        dataset.withColumn("words", split(col("value"),"\\W+")).drop("value").withColumn("word",explode(col("words"))).drop("words").show();


    }
}
