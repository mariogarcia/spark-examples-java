package github.mariogarcia.spark.simple;

import static org.junit.Assert.assertThat;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.equalTo;
import static github.mariogarcia.spark.common.Configuration.getConf;

import scala.Tuple2;
import org.junit.Test;
import java.util.List;
import org.apache.spark.SparkConf;
import github.mariogarcia.spark.common.Collections;

/**
 * Covers transformation functions used in the Spark task
 *
 * @since 0.1.0
 */
public class SparkWordCountTests {

    @Test
    public void testFlatPairs() {
        String lineSample = "50 Android Hacks|android|9781617290565";
        List<String> tuple = Collections.toStream(SparkWordCount.flatPairs(lineSample))
            .map(pair -> pair._1)
            .collect(toList());

        assertThat(tuple, hasItems("50", "Android", "Hacks", "android", "9781617290565"));
    }

    @Test
    public void testGetConf() {
        SparkConf conf = getConf("WordCount");

        assertThat(conf.get("spark.master"), equalTo("local"));
        assertThat(conf.get("spark.app.name"), equalTo("WordCount"));
    }
}
