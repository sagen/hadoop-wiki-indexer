package no.sagen.wikifind.indexer.mapper;

import edu.jhu.nlp.wikipedia.WikiTextParser;
import no.sagen.wikifind.indexer.transfer.DocumentTerm;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static no.sagen.wikifind.common.Parser.extractTagContents;
import static no.sagen.wikifind.common.Parser.parse;
import static no.sagen.wikifind.indexer.mapper.ParsingUtils.*;

public class TermsInWikiPageMapper extends Mapper<Text, Text, Text, DocumentTerm> {
    private static long written = 0;

    @Override
    public void map(Text nonsense, Text value, Context context) throws IOException, InterruptedException {
        String extractedTitle = extractTagContents(value, TITLE_TAG_NAME);
        if(!shouldDocumentBeIndexedBasedOnTitle(extractedTitle)){
            return;
        }
        //reporter.setStatus("Mapping " + extractedTitle);
        String text = extractedTitle + " " + getTextContent(value);
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
        long beforeWritten = written;
        for(Map.Entry<String, Integer> entry : termCount.entrySet()){
            context.write(new Text(entry.getKey()), new DocumentTerm(id, entry.getValue(), asList(parsedTitle).contains(entry.getKey())));
            written +=  13 + (4 * termCount.size());
            System.out.println("Mapped " + written + " bytes");
        }
        System.out.println("Written for " + extractedTitle + " : " + (written - beforeWritten));
        context.setStatus("Done mapping " + extractedTitle + " with " + termCount.size() + " unique terms");
    }

    private String getTextContent(Text value) throws UnsupportedEncodingException {
        String extracted = extractTagContents(value, TEXT_TAG_NAME);
        if(extracted == null || extracted.isEmpty()){
            return null;
        }
        return new WikiTextParser(extracted).getPlainText();
    }
}
