package no.sagen.wikifind.indexer.transfer;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class TfIdfWritable implements Writable {

    private Map<Long, Double> values = new HashMap<>();
    private int df;

    public TfIdfWritable(int df) {
        this.df = df;
    }

    public void add(Long docId, Double tfidf){
        values.put(docId, tfidf);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(values.size());
        for(Map.Entry<Long, Double> docEntry : values.entrySet()){
            out.writeLong(docEntry.getKey());
            out.writeDouble(docEntry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        values.clear();
        int count = in.readInt();
        for(int x = 0; x < count; x++){
            long key = in.readLong();
            values.put(key, in.readDouble());
        }
    }
    public byte[] toByteArray(){
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + (12 * values.size()));
        byteBuffer.putInt(df);
        for(Map.Entry<Long, Double> docEntry : values.entrySet()){
            byteBuffer.putLong(docEntry.getKey());
            byteBuffer.putFloat(docEntry.getValue().floatValue());
        }
        return byteBuffer.array();
    }
}
