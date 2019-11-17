package packt1;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class UtilitiesJava {

    public static String separator = File.separator;
    private static String novellaLocation = "/Users/a/IdeaProjects/The-Spark-Workshop/resources/mapreduce/HoD.txt"; // ToDo: Specify
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

    public static void main(String... args) {
        System.out.println("Test");
    }
}
