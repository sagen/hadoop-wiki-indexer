package no.sagen.wikifind.retriever;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import no.sagen.wikifind.common.RiakConnection;

import java.nio.ByteBuffer;
import java.util.*;

import static java.util.Arrays.asList;
import static no.sagen.wikifind.common.Parser.parse;

public class Retriever {
    public static List<CandidateDocument> retrieve(String phrase) throws RiakException {
        Set<String> terms = new HashSet<>(asList(parse(phrase)));
        IRiakClient client = RiakConnection.getFetchClient();

        Bucket tfidfBucket = client.fetchBucket("tfidf").execute();

        Map<Integer, CandidateDocument> candidateDocs = new HashMap<>();
        Query query = new Query(terms);
        Bucket titleBucket = client.fetchBucket("title").execute();

        for(String term : terms){
            ByteBuffer buffer = ByteBuffer.wrap(tfidfBucket.fetch(term).execute().getValue());
            query.add(term, buffer.getFloat(), false);
            while(buffer.hasRemaining()){
                int docKey = buffer.getInt();
                float tfidf = buffer.getFloat();
                boolean boost = buffer.get() == 1;
                CandidateDocument doc = candidateDocs.get(docKey);
                if(doc == null){
                    doc = new CandidateDocument(docKey);
                    doc.setEuclideanNorm(EuclideanNormStore.getNorm(docKey));
                    candidateDocs.put(docKey, doc);
                    IRiakObject titleResult = titleBucket.fetch(Integer.toString(doc.getDocId())).execute();
                    if(titleResult != null){
                        String title = titleResult.getValueAsString();
                        doc.setTitle(title);
                    }
                }
                doc.add(term, tfidf, boost);
            }
        }
        for(CandidateDocument doc : candidateDocs.values()){
            query.setSimilarityOnDoc(doc);
        }
        List<CandidateDocument> result = new ArrayList<>(candidateDocs.values());
        Collections.sort(result);

        List<CandidateDocument> res = new ArrayList<>();
        int i = 0;
        for(CandidateDocument doc : result){
            if(i++ == 10) break;
            doc.setTitle(titleBucket.fetch(Integer.toString(doc.getDocId())).execute().getValueAsString());
            System.out.println(doc);
            res.add(doc);
        }
        return res;
    }


}
