package no.sagen.wikifind.indexer.reducer;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IdToTitleReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable id, Iterable<Text> title, Context context) throws IOException, InterruptedException {
        context.write(id, title.iterator().next());
    }
}
