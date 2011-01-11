package org.lilyproject.testclientfw;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Words {
    private static List<String> words = new ArrayList<String>(100000);

    static {
        try {
            InputStream is = Words.class.getResourceAsStream("wordlist-50.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String word;
            while ((word = reader.readLine()) != null) {
                // The lowerCase is a simple trick to avoid that words like OR, AND, NOT cause
                // the SOLR query parser to fail.
                words.add(word.toLowerCase());
            }
            is.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static String get() {
        return words.get((int)(Math.floor(Math.random() * words.size())));
    }
}
