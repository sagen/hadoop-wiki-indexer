package no.sagen.wikifind.indexer.output;

import com.basho.riak.client.RiakException;
import com.basho.riak.pbc.RiakObject;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class IdToTitleRecordWriter extends RiakRecordWriter<IntWritable, Text> {
    public IdToTitleRecordWriter() throws RiakException, IOException {}

    @Override
    public void write(IntWritable longWritable, Text text) throws IOException {
        add(new RiakObject("title", longWritable.toString(), text.toString()));
    }

}
