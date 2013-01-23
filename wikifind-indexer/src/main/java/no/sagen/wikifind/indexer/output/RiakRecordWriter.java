package no.sagen.wikifind.indexer.output;

import com.basho.riak.client.RiakException;
import com.basho.riak.pbc.RequestMeta;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakObject;
import no.sagen.wikifind.common.RiakConnection;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class RiakRecordWriter<K, V> extends RecordWriter<K, V> {
    private RiakClient riakClient;
    private long count = 0;
    private List<RiakObject> buffer = new ArrayList<>(100000);

    public RiakRecordWriter() throws RiakException, IOException {
        riakClient = RiakConnection.getPutClient();
    }


    protected void add(RiakObject obj) throws IOException {
        buffer.add(obj);
        if(count++ % 100000 == 0){
            riakClient.store(buffer.toArray(new RiakObject[buffer.size()]), new RequestMeta());
            System.out.println("Saved 100000 entries to DB");
            buffer.clear();
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        if(!buffer.isEmpty()){
            riakClient.store(buffer.toArray(new RiakObject[buffer.size()]), new RequestMeta());
            buffer.clear();
        }
        context.setStatus(count + " rows written to DB!");
    }
}
