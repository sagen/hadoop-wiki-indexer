package no.sagen.wikifind.indexer.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static java.lang.Integer.parseInt;
import static no.sagen.wikifind.common.Parser.extractTagContents;


public class IdToTitleMapper extends Mapper<Text, Text, IntWritable, Text> {
    @Override
    public void map(Text nonsense, Text text, Context context) throws IOException, InterruptedException {
        String title = extractTagContents(text, "title");
        if(title.startsWith("Wikipedia:") || title.startsWith("Kategori:") || title.startsWith("Mal:")){
            return;
        }
        int id = parseInt(extractTagContents(text, "id"));
        context.setStatus("Mapped " + id +  " : " + title);
        context.write(new IntWritable(id), new Text(title));
    }
}
