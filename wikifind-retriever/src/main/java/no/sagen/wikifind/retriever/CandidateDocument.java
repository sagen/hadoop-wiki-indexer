package no.sagen.wikifind.retriever;


import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;

@XmlRootElement
public class CandidateDocument implements Comparable<CandidateDocument>{

    Map<String, Float> tfidfs = new HashMap<>();

    double similarity;
    final int docId;

    float euclideanNorm;

    String title;
    public CandidateDocument(int docId) {
        this.docId = docId;
    }

    public int getDocId() {
        return docId;
    }

    void add(String term, float tfidf, boolean boost){
        tfidfs.put(term, tfidf * (boost ? 1.5f : 1f));

    }

    public void setEuclideanNorm(float euclideanNorm) {
        this.euclideanNorm = euclideanNorm;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    protected void setSimilarityToQuery(double similarity){
        this.similarity = similarity;
    }
    @Override
    public int compareTo(CandidateDocument o) {
        return Double.compare(o.similarity, similarity);
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof CandidateDocument) && docId == ((CandidateDocument)o).docId;
    }

    @Override
    public int hashCode() {
        return docId;
    }

    @Override
    public String toString() {
        return "CandidateDocument{" +
                "tfidfs=" + tfidfs +
                ", similarity=" + similarity +
                ", docId=" + docId +
                ", title='" + title + '\'' +
                '}';
    }

    public Map toMap(){
        HashMap vals = new HashMap();
        vals.put("tfidfs", tfidfs);
        vals.put("docid", docId);
        vals.put("similarity", similarity);
        vals.put("title", title);
        return vals;
    }
}
