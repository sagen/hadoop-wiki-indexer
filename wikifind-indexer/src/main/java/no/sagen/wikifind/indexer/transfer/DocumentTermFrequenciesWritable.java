package no.sagen.wikifind.indexer.transfer;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;

public class DocumentTermFrequenciesWritable implements Writable {
    public List<Integer> termFrequencies;

    public DocumentTermFrequenciesWritable() {}

    public DocumentTermFrequenciesWritable(Collection<Integer> termFrequencies) {
        this.termFrequencies = new ArrayList(termFrequencies);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(termFrequencies.size());
        for(Integer entry : termFrequencies){
            dataOutput.writeInt(entry);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        termFrequencies = new ArrayList<>();
        int size = dataInput.readInt();
        for(int x = 0; x < size; x++){
            termFrequencies.add(dataInput.readInt());
        }
    }
}
