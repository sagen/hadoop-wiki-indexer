package no.sagen.wikifind.common;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class CassandraDatastoreMutatorFactory {

    private static final String CASSANDRA_HOST = "localhost:9160";
    private static final String CASSANDRA_KEYSPACE = "wikifind";
    private static Keyspace keyspace;

    public static Mutator mutator() {
        if(keyspace == null){
            Cluster cluster = HFactory.getOrCreateCluster("cluster", CASSANDRA_HOST);
            keyspace = HFactory.createKeyspace(CASSANDRA_KEYSPACE, cluster);
        }
        return HFactory.createMutator(keyspace, new StringSerializer());
    }

}
