package no.sagen.wikifind.indexer.output;


import com.basho.riak.client.RiakException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class IdToTitleOutputFormat implements OutputFormat {
    @Override
    public RecordWriter getRecordWriter(FileSystem fileSystem, JobConf entries, String s, Progressable progressable) throws IOException {
        try {
            return new IdToTitleRecordWriter();
        } catch (RiakException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf entries) throws IOException {}
}
