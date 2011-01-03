package org.lilyproject.testclientfw;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Words {
    private static List<String> words = new ArrayList<String>(100000);

    static {
        try {
            InputStream is = Words.class.getResourceAsStream("american-words.95");
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String word;
            while ((word = reader.readLine()) != null) {
                words.add(word);
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
