package no.sagen.wikifind.retriever.server;

import com.basho.riak.client.RiakException;
import no.sagen.wikifind.retriever.Retriever;

import javax.annotation.PostConstruct;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/search")
public class SearchResource {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List search(@QueryParam("q") String query) throws RiakException {
        return Retriever.retrieve(query);
    }

}
