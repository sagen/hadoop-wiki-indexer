import com.basho.riak.client.RiakException;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakObject;
import no.sagen.wikifind.common.RiakConnection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException, RiakException {

        String word = "arbeid";

        RiakClient client = RiakConnection.getClient();
        RiakObject resp = client.fetch("tfidf", word)[0];
        ByteBuffer byteBuffer = ByteBuffer.allocate(resp.getValue().size());
        resp.getValue().copyTo(byteBuffer);
        byteBuffer.rewind();
        Map<Long, Double> tfidfs = new HashMap<>();
        while(byteBuffer.hasRemaining()){
            Long key = byteBuffer.getLong();
            double value = byteBuffer.getDouble();
            tfidfs.put(key, value);
            System.out.println(key + " : " + value);
        }



    }

}
