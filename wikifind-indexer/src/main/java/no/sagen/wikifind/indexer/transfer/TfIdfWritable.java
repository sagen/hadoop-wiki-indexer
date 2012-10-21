package no.sagen.wikifind.indexer.transfer;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;

import static java.nio.ByteBuffer.allocate;

public class TfIdfWritable implements Writable {
    class DocTerm implements Comparable<DocTerm>{
        float tfidf; int docId; boolean boost;
        DocTerm(int docId, float tfidf, boolean boost) {
            this.tfidf = tfidf;
            this.docId = docId;
            this.boost = boost;
        }
        @Override
        public boolean equals(Object o) {return ((o instanceof DocTerm) && ((DocTerm)o).docId == docId);}
        @Override
        public int hashCode() {return docId;}
        @Override
        public int compareTo(DocTerm o) {return tfidf < o.tfidf ? -1 : tfidf > o.tfidf ? 1 : 0;}
    }
    private HashSet<DocTerm> docs = new HashSet<>();
    private float idf;

    public TfIdfWritable(float idf) {
        this.idf = idf;
    }

    public void add(int docId, float tfidf, boolean boost){
        docs.add(new DocTerm(docId, tfidf, boost));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(docs.size());
        for (DocTerm term : docs) {
            out.writeInt(term.docId);
            out.writeFloat(term.tfidf);
            out.writeBoolean(term.boost);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        docs.clear();
        int count = in.readInt();
        for(int x = 0; x < count; x++){
            int docKey = in.readInt();
            float tfidf = in.readFloat();
            add(docKey, tfidf, in.readBoolean());
        }
    }
    public byte[] toByteArray(){
        ByteBuffer byteBuffer = allocate(4 + (9 * docs.size()));
        byteBuffer.putFloat(idf);
        for(DocTerm doc : docs){
            byteBuffer.putInt(doc.docId);
            byteBuffer.putFloat(doc.tfidf);
            byteBuffer.put((byte) (doc.boost ? 1 : 0));
        }
        return byteBuffer.array();
    }
}
