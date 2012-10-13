package no.sagen.wikifind.indexer.transfer;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DocumentTerm implements Writable{
    long docId;
    int documentFrequency;
    int termFrequency;

    public DocumentTerm() {}
    public DocumentTerm(long docId, int termFrequency) {
        this.docId = docId;
        this.termFrequency = termFrequency;
    }

    public void setDocumentFrequency(int documentFrequency) {
        this.documentFrequency = documentFrequency;
    }

    public long getDocId() {
        return docId;
    }

    public int getDocumentFrequency() {
        return documentFrequency;
    }

    public int getTermFrequency() {
        return termFrequency;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(documentFrequency);
        dataOutput.writeInt(termFrequency);
        dataOutput.writeLong(docId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        documentFrequency = dataInput.readInt();
        termFrequency = dataInput.readInt();
        docId = dataInput.readLong();
    }
}
