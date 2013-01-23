package no.sagen.wikifind.retriever;

import java.util.Set;

import static java.lang.Math.sqrt;

public class Query extends CandidateDocument{
    Set<String> terms;

    public Query(Set<String> terms) {
        super(-1);
        this.terms = terms;


    }
    public void setSimilarityOnDoc(CandidateDocument doc) {
        double top = 0;
        double bottomSumQuery = 0;
        double bottomSumDoc = doc.euclideanNorm;
        for(String term : terms){
            Float docTermTfIdf = doc.tfidfs.get(term);
            if(docTermTfIdf == null){
                docTermTfIdf = 0f;
            }
            float queryTfIdf = 40f;//tfidfs.get(term);
            top += queryTfIdf * docTermTfIdf;
            bottomSumQuery += queryTfIdf * queryTfIdf;
        }
        doc.setSimilarityToQuery(top / (bottomSumDoc * sqrt(bottomSumQuery)));
    }
}
