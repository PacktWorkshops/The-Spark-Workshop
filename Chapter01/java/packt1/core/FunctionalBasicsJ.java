package packt1.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FunctionalBasicsJ {

    public static void main(String... args) {
        List<String> words = new ArrayList<>(Arrays.asList("Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the", "untouched", "expanse", "of", "their", "background"));
        List<Integer> mapped1 = words.stream().map(word -> word.length()).collect(Collectors.toList()); // lambda
        List<Integer> mapped2 = words.stream().map(String::length).collect(Collectors.toList()); // using method reference instead of lambda
        System.out.println(mapped1);
        System.out.println(mapped2);
    }

}