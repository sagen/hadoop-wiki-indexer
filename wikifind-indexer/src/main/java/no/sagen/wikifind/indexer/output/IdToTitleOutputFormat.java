package no.sagen.wikifind.indexer.output;


import com.basho.riak.client.RiakException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IdToTitleOutputFormat extends FileOutputFormat {
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        try {
            return new IdToTitleRecordWriter();
        } catch (RiakException e) {
            throw new RuntimeException(e);
        }
    }

}
