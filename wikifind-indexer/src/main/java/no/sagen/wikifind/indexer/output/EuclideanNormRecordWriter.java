package no.sagen.wikifind.indexer.output;

import com.basho.riak.client.RiakException;
import com.basho.riak.pbc.RiakObject;
import no.sagen.wikifind.indexer.transfer.DocumentTermFrequenciesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.nio.ByteBuffer;

public class EuclideanNormRecordWriter extends RiakRecordWriter<IntWritable, FloatWritable> {
    public EuclideanNormRecordWriter() throws RiakException, IOException {}

    @Override
    public void write(IntWritable intWritable, FloatWritable euclideanLength) throws IOException {
        ByteBuffer valueBuffer = ByteBuffer.allocate(4);
        valueBuffer.putFloat(euclideanLength.get());
        add(new RiakObject("euclideanlength", Integer.toString(intWritable.get()), valueBuffer.array()));
    }
}
