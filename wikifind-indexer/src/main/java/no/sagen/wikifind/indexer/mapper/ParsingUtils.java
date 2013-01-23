package no.sagen.wikifind.indexer.mapper;

import java.util.HashMap;
import java.util.Map;

public class ParsingUtils {

    static final String TEXT_TAG_NAME = "text";
    static final String TITLE_TAG_NAME = "title";
    static final String ID_TAG_NAME = "id";

    static boolean shouldDocumentBeIndexedBasedOnTitle(String title){
        return !(title.startsWith("Wikipedia:") || title.startsWith("Kategori:") || title.startsWith("Mal:"));
    }

    static void count(Map<String, Integer> termCount, String stemmedWord) {
        Integer val = termCount.get(stemmedWord);
        if(val != null){
            termCount.put(stemmedWord, val + 1);
        }else{
            termCount.put(stemmedWord, 1);
        }
    }
}
