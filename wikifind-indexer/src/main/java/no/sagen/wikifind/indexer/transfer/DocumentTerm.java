package no.sagen.wikifind.indexer.transfer;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DocumentTerm implements Writable{
    int docId;
    int termFrequency;
    boolean inTitle;

    public DocumentTerm() {}
    public DocumentTerm(DocumentTerm copyFrom) {
        this.docId = copyFrom.docId;
        this.termFrequency = copyFrom.termFrequency;
        this.inTitle = copyFrom.inTitle;
    }
    public DocumentTerm(int docId, int termFrequency, boolean inTitle) {
        this.docId = docId;
        this.termFrequency = termFrequency;
        this.inTitle = inTitle;
    }


    public int getDocId() {
        return docId;
    }


    public int getTermFrequency() {
        return termFrequency;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(termFrequency);
        dataOutput.writeInt(docId);
        dataOutput.writeBoolean(inTitle);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        termFrequency = dataInput.readInt();
        docId = dataInput.readInt();
        inTitle = dataInput.readBoolean();
    }

    public boolean getInTitle() {
        return inTitle;
    }
}
