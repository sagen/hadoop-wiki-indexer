package no.sagen.wikifind.indexer.mapper;

import no.sagen.wikifind.common.Parser;
import no.sagen.wikifind.indexer.transfer.DocumentTermFrequenciesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Integer.parseInt;
import static no.sagen.wikifind.common.Parser.extractTagContents;
import static no.sagen.wikifind.common.Parser.parse;
import static no.sagen.wikifind.indexer.mapper.ParsingUtils.*;

public class MapperForEuclideanNorm extends Mapper<Text, Text, IntWritable, DocumentTermFrequenciesWritable> {
    @Override
    public void map(Text nonsense, Text text, Context context) throws IOException, InterruptedException {
        String title = extractTagContents(text, TITLE_TAG_NAME);
        String content = extractTagContents(text, TEXT_TAG_NAME);
        int id = parseInt(extractTagContents(text, ID_TAG_NAME));
        if(!shouldDocumentBeIndexedBasedOnTitle(title)){
            return;
        }
        String[] parsedText = parse(content + " " + title);
        Map<String, Integer> count = new HashMap<>();
        for(String term : parsedText){
            count(count, term);
        }
        context.setStatus("Done with doc " + title);
        context.write(new IntWritable(id), new DocumentTermFrequenciesWritable(count.values()));
    }

}
