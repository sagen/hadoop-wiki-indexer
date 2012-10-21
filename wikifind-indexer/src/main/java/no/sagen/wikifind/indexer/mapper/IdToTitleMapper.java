package no.sagen.wikifind.indexer.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

import static java.lang.Integer.parseInt;
import static no.sagen.wikifind.common.Parser.extractTagContents;

public class IdToTitleMapper implements Mapper<Text, Text, IntWritable, Text> {
    @Override
    public void map(Text nonsense, Text text, OutputCollector<IntWritable, Text> out, Reporter reporter) throws IOException {
        String title = extractTagContents(text, "title");
        if(title.startsWith("Wikipedia:") || title.startsWith("Kategori:") || title.startsWith("Mal:")){
            return;
        }
        int id = parseInt(extractTagContents(text, "id"));
        reporter.setStatus("Mapped " + id +  " : " + title);
        out.collect(new IntWritable(id), new Text(title));
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(JobConf entries) {
    }
}
