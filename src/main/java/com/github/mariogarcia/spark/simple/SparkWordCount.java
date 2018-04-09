package github.mariogarcia.spark.simple;

import static github.mariogarcia.spark.common.Configuration.getConf;
import static github.mariogarcia.spark.common.ResourceUtils.getResourceURI;

import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.net.URISyntaxException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import github.mariogarcia.spark.common.Collections;

/**
 * Example showing how to count words from a csv file
 *
 * @since 0.1.0
 */
public final class SparkWordCount {

    private SparkWordCount() {
        // Prevents JaCoCo to check class instantiation
    }

    /**
     * Main entry point
     *
     * @param args execution arguments
     * @since 0.1.0
     */
    public static void main(String args[]) throws URISyntaxException {
        String fileURI = getResourceURI("/books.csv");

        new JavaSparkContext(getConf("WordCount"))
            .textFile(fileURI)
            .flatMapToPair(SparkWordCount::flatPairs)
            .reduceByKey(Collections::sum)
            .saveAsTextFile("/tmp/spark/WordCount");
    }

    /**
     * Takes all words in a given line and creates pairs of type
     * (word, 1)
     *
     * @param line the text line in the text file
     * @return an {@link Iterator} of (word, 1)
     * @since 0.1.0
     */
    public static Iterator<Tuple2<String, Integer>> flatPairs(String line) {
        return Arrays
            .asList(line.split("\\|"))
            .stream()
            .flatMap(word -> Arrays.asList(word.split(" ")).stream())
            .map(word -> new Tuple2<String, Integer>(word, 1))
            .iterator();
    }
}
