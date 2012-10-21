package no.sagen.wikifind.common;


import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.pbc.RiakClient;

import java.io.IOException;

public class RiakConnection {

    private static RiakClient client;

    public static RiakClient getPutClient() throws RiakException, IOException {
        if(client == null){
            int i = 0;
            client = new RiakClient("localhost");
//            KeySource keys = client.listKeys(ByteString.copyFromUtf8("tfidf"));
//            while(keys.hasNext()){
//                ByteString next = keys.next();
//                System.out.println("deleting " + ++i + " " + next.toStringUtf8());
//                client.delete(ByteString.copyFromUtf8("tfidf"), next);
//            }
            return client;
        }
        return client;
            //IRiakClient riakClient = RiakFactory.pbcClient();
            //bucket = riakClient.createBucket("tfidf").execute();
    }

    public static IRiakClient getFetchClient() throws RiakException {
        return RiakFactory.pbcClient();
    }
}
