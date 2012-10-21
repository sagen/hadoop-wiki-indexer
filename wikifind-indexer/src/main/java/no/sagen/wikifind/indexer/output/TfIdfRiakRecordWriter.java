package no.sagen.wikifind.indexer.output;

import com.basho.riak.client.RiakException;
import com.basho.riak.pbc.RiakObject;
import no.sagen.wikifind.indexer.transfer.TfIdfWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;


public class TfIdfRiakRecordWriter extends RiakRecordWriter<Text, TfIdfWritable> {

    public TfIdfRiakRecordWriter() throws RiakException, IOException {}

    @Override
    public void write(Text key, TfIdfWritable value) throws IOException {
        add(new RiakObject("tfidf", key.toString(), value.toByteArray()));
    }



}
