package no.sagen.wikifind.indexer.mapper;

import edu.jhu.nlp.wikipedia.WikiTextParser;
import no.sagen.wikifind.indexer.transfer.TermFrequencyInDocument;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.regex.Pattern;

import static java.util.Arrays.copyOfRange;
import static java.util.regex.Pattern.compile;

public class TermsInWikiPageMapper implements Mapper<Text, Text, Text, TermFrequencyInDocument> {
    private static final String TEXT_TAG_NAME = "text";
    private static final String TITLE_TAG_NAME = "title";
    private static final String ID_TAG_NAME = "id";
    private static final Pattern SPLIT_PATTERN = compile("\\s+");
    private static final Pattern CLEAN_PATTERN = compile("[\\p{P}\\p{S}]+");

    @Override
    public void map(Text key, Text value, OutputCollector<Text, TermFrequencyInDocument> output, Reporter reporter) throws IOException {
        String extractedTitle = extractFromTag(value, TITLE_TAG_NAME);
        reporter.setStatus("Mapping " + extractedTitle);
        if(extractedTitle == null){
            return;
        }
        String text = extractedTitle + getTextContent(value);
        long id = Long.parseLong(extractFromTag(value, ID_TAG_NAME));
        HashMap<String, Integer> termCount = new HashMap<String, Integer>();
        NorwegianStemmer stemmer = new NorwegianStemmer();
        for(String word : SPLIT_PATTERN.split(text)){
            count(termCount, stem(stemmer, word));
        }
        for(Map.Entry<String, Integer> entry : termCount.entrySet()){
            output.collect(new Text(entry.getKey()), new TermFrequencyInDocument(id, entry.getValue()));
        }
        reporter.setStatus("Done mapping " + extractedTitle);
    }

    private String getTextContent(Text value) throws UnsupportedEncodingException {
        String extracted = extractFromTag(value, TEXT_TAG_NAME);
        if(extracted == null || extracted.isEmpty()){
            return null;
        }
        return CLEAN_PATTERN.matcher(new WikiTextParser(extracted).getPlainText().toLowerCase()).replaceAll("");
    }

    private void count(HashMap<String, Integer> termCount, String stemmedWord) {
        if(termCount.containsKey(stemmedWord)){
            termCount.put(stemmedWord, termCount.get(stemmedWord) + 1);
        }else{
            termCount.put(stemmedWord, 1);
        }
    }

    private String stem(NorwegianStemmer stemmer, String word) {
        stemmer.setCurrent(word);
        stemmer.stem();
        return stemmer.getCurrent();
    }

    private String extractFromTag(Text text, String tagName) throws UnsupportedEncodingException {
        String endTag = "</" + tagName + ">";
        String startTagStart = "<" + tagName;
        int startPos = findTextStartPos(text.getBytes(), startTagStart.getBytes("UTF-8"));
        int endPos = text.find(endTag);
        if(startPos == -1 || endPos == -1){
            return null;
        }
        return new String(copyOfRange(text.getBytes(), startPos, endPos), "UTF-8");
    }

    private int findTextStartPos(byte[] inputBytes, byte[] tagStart){
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
    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void configure(JobConf job) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
