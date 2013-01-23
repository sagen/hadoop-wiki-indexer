package no.sagen.wikifind.retriever;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import no.sagen.wikifind.common.RiakConnection;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class EuclideanNormStore {
    public static Map<Integer, Float> norms = new HashMap<>();

    public static Float getNorm(Integer docId) throws RiakException {
        if(norms.isEmpty()){
            loadEuclideanNorms();
        }
        return norms.get(docId);
    }

    private static void loadEuclideanNorms() throws RiakException {
        IRiakClient client = RiakConnection.getFetchClient();
        Bucket euclideanNormBucket = client.fetchBucket("euclideanlength").execute();
        for(String key : euclideanNormBucket.keys()){
            ByteBuffer byteBuffer = ByteBuffer.wrap(euclideanNormBucket.fetch(key).execute().getValue());
            norms.put(Integer.parseInt(key), byteBuffer.getFloat());
        }

    }
}
