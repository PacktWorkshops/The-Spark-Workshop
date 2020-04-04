package Utilities01;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class HelperJava {

    public static String separator = File.separator;
    private static String novellaLocation = "/Users/a/IdeaProjects/The-Spark-Workshop/resources/HoD.txt"; // ToDo: change path
    public static Path novellaPath = new Path(novellaLocation);

    public static StringTokenizer tokenizeSimple(String text) {
        StringTokenizer tokenizer = new StringTokenizer(text);
        return tokenizer;
    }

    public static List<String> createBigrams(String text) {
        List<String> bigrams = new ArrayList<>();
        StringTokenizer tokenizer = tokenizeSimple(text);
        String previousToken = "";
        while (tokenizer.hasMoreTokens()) {
            String currentToken = tokenizer.nextToken();
            if (previousToken.isEmpty()) {
                previousToken = currentToken;
                continue;
            }
            bigrams.add(previousToken + " " + currentToken);
            previousToken = currentToken;
        }
        return bigrams;
    }

    public static SparkSession createSession(int numThreads, String name) {
        return SparkSession.builder()
                .master("local[" + numThreads + "]") // program simulates a single executor with numThreads cores (one local JVM with numThreads threads)
                .appName(name)
                .getOrCreate();
    }

    public static void main(String... args) {
        System.out.println("Test");
    }
}
