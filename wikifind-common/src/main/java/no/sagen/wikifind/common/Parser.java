package no.sagen.wikifind.common;

import org.apache.hadoop.io.Text;

import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;

import static java.util.Arrays.copyOfRange;
import static java.util.regex.Pattern.compile;

public class Parser {

    private static final Pattern SPLIT_PATTERN = compile("\\s+");
    private static final Pattern CLEAN_PATTERN = compile("[\\p{P}\\p{S}]+");
    private static NorwegianStemmer stemmer = new NorwegianStemmer();

    public static String[] parse(String text){
        String clean = CLEAN_PATTERN.matcher(text.toLowerCase()).replaceAll("");
        String[] split = SPLIT_PATTERN.split(clean);
        for(int x = 0; x < split.length; x++){
            split[x] = stem(split[x]);
        }
        return split;
    }

    private static String stem(String word) {
        stemmer.setCurrent(word);
        stemmer.stem();
        return stemmer.getCurrent();
    }

    public static String extractTagContents(Text text, String tagName) throws UnsupportedEncodingException {
        String endTag = "</" + tagName + ">";
        String startTagStart = "<" + tagName;
        int startPos = findTextStartPos(text.getBytes(), startTagStart.getBytes("UTF-8"));
        int endPos = text.find(endTag);
        if(startPos == -1 || endPos == -1){
            return null;
        }
        return new String(copyOfRange(text.getBytes(), startPos, endPos), "UTF-8");
    }

    private static int findTextStartPos(byte[] inputBytes, byte[] tagStart){
        int posInTagStart = 0;
        int pos = -1;
        for(byte b : inputBytes){
            pos++;
            if(posInTagStart == tagStart.length - 1){
                if(b == '>'){
                    return pos + 1;
                }
            }else if(b == tagStart[posInTagStart]){
                posInTagStart++;
            }else{
                posInTagStart = 0;
            }
        }
        return -1;
    }

}
