package github.mariogarcia.spark.neo4j;

import static github.mariogarcia.spark.common.Configuration.getConf;
import static github.mariogarcia.spark.common.Configuration.getNeo4jContext;
import static github.mariogarcia.spark.common.ResourceUtils.getResourceURI;
import static java.lang.String.join;

import java.util.Map;
import java.util.HashMap;
import java.net.URISyntaxException;
import org.neo4j.spark.Neo4JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Reads a CSV file and loads its content in a Neo4j database
 *
 * @since 0.1.0
 */
public final class Neo4jCsvLoader {

    /**
     * Context name
     *
     * @since 0.1.0
     */
    public static final String CTX_NAME = "ReadBooks";

    private Neo4jCsvLoader() {
        // To prevent JaCoCo to check class instantiation
    }

    /**
     * Main entry point
     *
     * @param args execution arguments
     * @since 0.1.0
     */
    public static void main(String args[]) throws URISyntaxException {
        final Neo4JavaSparkContext neo4jContext = getNeo4jContext(CTX_NAME);
        final String fileURI = getResourceURI("/books.csv");

        new JavaSparkContext(getConf("Neo4j"))
            .textFile(fileURI)
            .map(Neo4jCsvLoader::createStatement)
            .map(Neo4jCsvLoader.executeStatements(neo4jContext))
            .saveAsTextFile("/tmp/spark/Neo4jImport");
    }

    /**
     * Converts a csv line to a Neo4j cypher statement
     *
     * @param line csv file containing a book's detail
     * @return an string containing a cypher statement
     * @since 0.1.0
     */
    public static String createStatement(String line) {
        String[] fields = line.split("\\|");

        String title = fields[0];
        String area = fields[1];
        String isbn = fields[2];
        String image = fields[3];
        String month = fields[4];
        String year = fields[5];
        String author = fields[6];

        String areaStmt         = join("", "MERGE (tech:Technology {name: \"", area, "\"})");
        String bookStmt         = join("", "MERGE (book:Book {title: \"", title, "\"})");
        String authorStmt       = join("", "MERGE (author:Author {name: \"", author, "\"})");
        String relationshipStmt = join("", "MERGE (tech)-[:HAS]->(book)<-[:WROTE {month: ", month, ", year: ", year ,"}]-(author)");
        String stmt             = join("\n", areaStmt, bookStmt, authorStmt, relationshipStmt);

        return stmt;
    }

    /**
     * Returns a function capable of executing cypher statements
     * against a given Neo4j context
     *
     * @param neo4jContext the Neo4j context
     * @return a function capable of executing cypher statements
     * @since 0.1.0
     */
    public static Function<String, JavaRDD<Row>> executeStatements(Neo4JavaSparkContext neo4jContext) {
        return (String cypher) -> {
            return neo4jContext.queryRow(cypher, new HashMap<String, Object>());
        };
    }
}
