package no.sagen.wikifind.indexer.output;

import com.basho.riak.client.RiakException;
import com.basho.riak.pbc.RequestMeta;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakObject;
import no.sagen.wikifind.common.RiakConnection;
import no.sagen.wikifind.indexer.transfer.TfIdfWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class TfIdfRiakRecordWriter implements RecordWriter<Text, TfIdfWritable> {
    long count = 0;
    List<RiakObject> buffer = new ArrayList<>(1000);
    private RiakClient riakClient;

    public TfIdfRiakRecordWriter() throws RiakException, IOException {
        riakClient = RiakConnection.getClient();
    }

    @Override
    public void write(Text key, TfIdfWritable value) throws IOException {
        buffer.add(new RiakObject("tfidf", key.toString(), value.toByteArray()));
        if(count++ % 100000 == 0){
            riakClient.store(buffer.toArray(new RiakObject[buffer.size()]), new RequestMeta());
            System.out.println("Saved 100000 entries to DB");
            buffer.clear();
        }
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        if(!buffer.isEmpty()){
            riakClient.store(buffer.toArray(new RiakObject[buffer.size()]), new RequestMeta());
            buffer.clear();
        }
        reporter.setStatus(count + " rows written to DB!");
    }

}
