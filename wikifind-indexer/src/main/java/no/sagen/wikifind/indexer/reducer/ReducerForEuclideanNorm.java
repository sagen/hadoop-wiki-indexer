package no.sagen.wikifind.indexer.reducer;


import no.sagen.wikifind.indexer.transfer.DocumentTermFrequenciesWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

import static java.lang.Math.sqrt;

public class ReducerForEuclideanNorm extends Reducer<IntWritable, DocumentTermFrequenciesWritable, IntWritable, FloatWritable> {
    private int totalNumberOfDocuments;

    @Override
    public void reduce(IntWritable intWritable, Iterable<DocumentTermFrequenciesWritable> documentTermFrequencies, Context context) throws IOException, InterruptedException {
        float result = 0f;
        DocumentTermFrequenciesWritable documentTermFrequency = documentTermFrequencies.iterator().next();
        for(int count : documentTermFrequency.termFrequencies){
             result += count * count;
        }
        result = (float) sqrt((double) result);
        context.setStatus("Reduced docId " + intWritable.get() + " with euclidean length " + result);
        context.write(new IntWritable(intWritable.get()), new FloatWritable(result));
    }

}
