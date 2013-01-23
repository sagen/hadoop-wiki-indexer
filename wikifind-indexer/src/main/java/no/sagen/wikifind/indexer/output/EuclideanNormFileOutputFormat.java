package no.sagen.wikifind.indexer.output;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class EuclideanNormFileOutputFormat extends FileOutputFormat {


    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new EuclideanNormFileRecordWriter();
    }
}
