package no.sagen.wikifind.retriever;

import com.basho.riak.client.RiakException;

import java.io.IOException;

import static no.sagen.wikifind.retriever.Retriever.retrieve;

public class Main {
    public static void main(String[] args) throws IOException, RiakException {
        String phrase = "jeg bedriver arbeid";
        retrieve(phrase);
    }
}
