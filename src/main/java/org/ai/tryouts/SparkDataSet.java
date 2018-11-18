package org.ai.tryouts;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


/**
 * (c) geekmj.org All right reserved
 * <p>
 * Main Spark Driver Program Class
 * It creates Resilient Distributed Dataset for a log file (external data source)
 * It does following operation on RDD
 * 1. Total no. of lines in log file
 * 2. Total characters in log file
 * 3. Total no. of URL in log file with HTML and GIF extension
 */
public class SparkDataSet {

    static Logger logger = LoggerFactory.getLogger(SparkDataSet.class);


    public static void main(String args[]) {


        //Iterator<String> it = getSingleRouteIterator();
        Iterator<String> it = getAllRoutes();


        showDataSet(JavaConverters.asScalaIteratorConverter(it).asScala().toSeq());
    }

    private static Iterator<String> getSingleRouteIterator() {
        URL jsonUrl = SparkDataSet.class.getResource("routesGermany400ml.json");
        File dir = new File(
                jsonUrl
                        .getPath()
                        .replace("routesGermany400ml.json", "") + "routes");

        logger.error("dir: {}", dir);

        List<String> files =
                Arrays.stream(dir.listFiles()).map(file -> file.getAbsolutePath()).collect(Collectors.toList());

        logger.error("files: {}", files);

        return files.iterator();
    }

    private static Iterator<String> getAllRoutes() {
        try {
            URL jsonUrl = SparkDataSet.class.getClassLoader().getResource("routesGermany400ml.json");

            logger.error("json: ", jsonUrl);

            Gson gson = new Gson();
            JsonReader reader = new JsonReader(new FileReader(jsonUrl.getPath()));
            JsonParser jsonParser = new JsonParser();
            JsonArray jsonRoutes = (JsonArray) jsonParser.parse(reader);

            Iterable<JsonElement> iterable = () -> jsonRoutes.iterator();
            List<String> routes =
                    StreamSupport
                            .stream(iterable.spliterator(), true)
                            .map(route -> {
                                return route.toString();
                            })
                            .collect(Collectors.toList());

            logger.error("routes: {}", routes);

            return routes.iterator();
        } catch (
                Exception e) {
            logger.error("Error: {}", e);
            return null;
        }

    }

    private static void showDataSet(Seq<String> files) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Test app");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();


        Dataset<String> anotherPeopleDataset = spark.createDataset(files, Encoders.STRING());
        Dataset<Row> df = spark.read().json(anotherPeopleDataset);

        //Dataset<Row> df = spark.read().json(files);

        df.printSchema();

        // Displays the content of the DataFrame to stdout
        df.show();

        df.select("rating").show();
    }

    private static void sparkTest1() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("DataVec Join Example");
        JavaSparkContext sc = new JavaSparkContext(conf);


        String logFile = "/Users/felipelopez/Documents/climbing-map/apache-spark-examples/01-getting-started/build.gradle"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }

    private static void test(String arg) {
        /* Define Spark Configuration */
        SparkConf conf = new SparkConf().setAppName("01-Getting-Started").setMaster(arg);

        /* Create Spark Context with configuration */
        JavaSparkContext sc = new JavaSparkContext(conf);

        /* Create a Resilient Distributed Dataset for a log file
         * Each line in log file become a record in RDD
         * */
        JavaRDD<String> lines = sc.textFile(
                "/Users/felipelopez/Documents/climbing-map/apache-spark-examples/data/apache-log04-aug-31-aug-2011-nasa.log");

        System.out.println("Total lines in log file " + lines.count());

        /* Map operation -> Mapping number of characters into each line as RDD */
        JavaRDD<Integer> lineCharacters = lines.map(s -> s.length());
        /* Reduce operation -> Calculating total characters */
        int totalCharacters = lineCharacters.reduce((a, b) -> a + b);

        System.out.println("Total characters in log file " + totalCharacters);

        /* Reduce operation -> checking each line for .html character pattern */
        System.out.println("Total URL with html extension in log file "
                + lines.filter(oneLine -> oneLine.contains(".html")).count());

        /* Reduce operation -> checking each line for .gif character pattern */
        System.out.println("Total URL with gif extension in log file "
                + lines.filter(oneLine -> oneLine.contains(".gif")).count());

        sc.close();
    }
}
