package github.mariogarcia.spark.common;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.neo4j.spark.Neo4JavaSparkContext;

/**
 * Configuration related functions
 *
 * @since 0.1.0
 */
public final class Configuration {

    private Configuration() {
        // NOTHING
    }

    /**
     * Sets the initial Spark configuration
     *
     * @return an instance of {@link SparkConf}
     * @since 0.1.0
     */
    public static SparkConf getConf(String appName) {
        return new SparkConf()
            .setMaster("local")
            .setAppName(appName);
    }

    public static Neo4JavaSparkContext getNeo4jContext(String appName) {
        SparkConf config = getConf(appName);
        SparkContext sparkContext = new SparkContext(config);

        return Neo4JavaSparkContext.neo4jContext(sparkContext);
    }
}
