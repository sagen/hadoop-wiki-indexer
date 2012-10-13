package no.sagen.wikifind.indexer.transfer;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TermFrequencyInDocument implements Writable {
    private long docId;
    private int frequence;

    public long getDocId() {
        return docId;
    }

    public int getFrequence() {
        return frequence;
    }

    public TermFrequencyInDocument() {
    }

    public TermFrequencyInDocument(long docId, int count) {
        this.docId = docId;
        this.frequence = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(frequence);
        out.writeLong(docId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        frequence = in.readInt();
        docId = in.readLong();
    }
}
