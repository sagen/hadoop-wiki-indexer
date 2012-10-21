package no.sagen.wikifind.indexer.reducer;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class IdToTitleReducer implements Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable id, Iterator<Text> title, OutputCollector<IntWritable, Text> out, Reporter reporter) throws IOException {
        out.collect(id, title.next());
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void configure(JobConf entries) {}
}
