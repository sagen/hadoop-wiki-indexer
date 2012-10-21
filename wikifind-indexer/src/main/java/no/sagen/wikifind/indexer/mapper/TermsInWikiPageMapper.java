package no.sagen.wikifind.indexer.mapper;

import edu.jhu.nlp.wikipedia.WikiTextParser;
import no.sagen.wikifind.indexer.transfer.DocumentTerm;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static no.sagen.wikifind.common.Parser.extractTagContents;
import static no.sagen.wikifind.common.Parser.parse;

public class TermsInWikiPageMapper implements Mapper<Text, Text, Text, DocumentTerm> {
    private static final String TEXT_TAG_NAME = "text";
    private static final String TITLE_TAG_NAME = "title";
    private static final String ID_TAG_NAME = "id";


    @Override
    public void map(Text key, Text value, OutputCollector<Text, DocumentTerm> output, Reporter reporter) throws IOException {
        String extractedTitle = extractTagContents(value, TITLE_TAG_NAME);
        if(extractedTitle.startsWith("Wikipedia:")){
            return;
        }
        reporter.setStatus("Mapping " + extractedTitle);
        String text = extractedTitle + getTextContent(value);
        int id = parseInt(extractTagContents(value, ID_TAG_NAME));
        HashMap<String, Integer> termCount = new HashMap<String, Integer>();
        String[] parsedContent = parse(text);
        String[] parsedTitle = parse(extractedTitle);
        for(String term : parsedContent){
            count(termCount, term);
        }
        for(String term : parsedTitle){
            count(termCount, term);
        }

        for(Map.Entry<String, Integer> entry : termCount.entrySet()){
            output.collect(new Text(entry.getKey()), new DocumentTerm(id, entry.getValue(), asList(parsedTitle).contains(entry.getKey())));
        }
        reporter.setStatus("Done mapping " + extractedTitle);
    }

    private String getTextContent(Text value) throws UnsupportedEncodingException {
        String extracted = extractTagContents(value, TEXT_TAG_NAME);
        if(extracted == null || extracted.isEmpty()){
            return null;
        }
        return new WikiTextParser(extracted).getPlainText();
    }

    private void count(HashMap<String, Integer> termCount, String stemmedWord) {
        Integer val = termCount.get(stemmedWord);
        if(val != null){
            termCount.put(stemmedWord, val + 1);
        }else{
            termCount.put(stemmedWord, 1);
        }
    }





    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(JobConf job) {
    }
}
