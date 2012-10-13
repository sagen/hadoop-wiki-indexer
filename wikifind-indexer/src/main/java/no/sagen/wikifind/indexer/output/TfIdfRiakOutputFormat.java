package no.sagen.wikifind.indexer.output;

import com.basho.riak.client.RiakException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class TfIdfRiakOutputFormat implements OutputFormat {

    public TfIdfRiakOutputFormat() {}

    @Override
    public RecordWriter getRecordWriter(FileSystem ignored, JobConf conf, String name, Progressable progress) throws IOException {
        try {
            return new TfIdfRiakRecordWriter();
        } catch (RiakException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    }
}
