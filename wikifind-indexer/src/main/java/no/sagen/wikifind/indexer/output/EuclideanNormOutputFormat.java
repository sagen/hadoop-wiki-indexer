package no.sagen.wikifind.indexer.output;

import com.basho.riak.client.RiakException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class EuclideanNormOutputFormat extends OutputFormat {


    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        try {
            return new EuclideanNormRecordWriter();
        } catch (RiakException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }
}
