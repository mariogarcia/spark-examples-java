package github.mariogarcia.spark.simple;

import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import github.mariogarcia.spark.common.ResourceUtils;

/**
 * Example showing how to count words from a csv file
 *
 * @since 0.1.0
 */
public class SparkWordCount {

    /**
     * Main entry point
     *
     * @param args execution arguments
     * @since 0.1.0
     */
    public static void main(String args[]) throws URISyntaxException {
        SparkConf sparkConfiguration = getConf();
        JavaSparkContext context = new JavaSparkContext(sparkConfiguration);
        String fileURI = ResourceUtils
            .loadClasspathResource("/books.csv")
            .toString();

        JavaRDD<String> input = context.textFile(fileURI);
        JavaPairRDD<String, Integer> pairs = input.flatMapToPair(SparkWordCount::flatPairs);
        JavaPairRDD<String, Integer> wordCountRDD = pairs.reduceByKey((v1, v2) -> v1 + v2);

        wordCountRDD.saveAsTextFile("/tmp/lala.txt");
    }

    /**
     * Sets the initial Spark configuration
     *
     * @return an instance of {@link SparkConf}
     * @since 0.1.0
     */
    static SparkConf getConf() {
        return new SparkConf()
            .setMaster("local")
            .setAppName("WordCount");
    }

    /**
     * Takes all words in a given line and creates pairs of type
     * (word, 1)
     *
     * @param line the text line in the text file
     * @return an {@link Iterator} of (word, 1)
     * @since 0.1.0
     */
    static Iterator<Tuple2<String, Integer>> flatPairs(String line) {
        return Arrays
            .asList(line.split("\\|"))
            .stream()
            .flatMap(word -> Arrays.asList(word.split(" ")).stream())
            .map(word -> new Tuple2<String, Integer>(word, 1))
            .iterator();
    }
}
