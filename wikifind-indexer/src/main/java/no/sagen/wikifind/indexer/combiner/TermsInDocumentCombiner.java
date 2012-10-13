package no.sagen.wikifind.indexer.combiner;

import no.sagen.wikifind.indexer.transfer.DocumentTerm;
import no.sagen.wikifind.indexer.transfer.TermFrequencyInDocument;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TermsInDocumentCombiner implements Reducer<Text, TermFrequencyInDocument, Text, DocumentTerm> {
    @Override
    public void reduce(Text text, Iterator<TermFrequencyInDocument> values, OutputCollector<Text, DocumentTerm> output, Reporter reporter) throws IOException {
        int docsContainingTerm = 0;
        Map<String, DocumentTerm> documentTerms = new HashMap<>();
        while (values.hasNext()){
            docsContainingTerm++;
            TermFrequencyInDocument termFrequencyInDoc = values.next();
            documentTerms.put(text.toString(), new DocumentTerm(termFrequencyInDoc.getDocId(), termFrequencyInDoc.getFrequence()));
        }
        for(Map.Entry<String, DocumentTerm> docTerm : documentTerms.entrySet()){
            docTerm.getValue().setDocumentFrequency(docsContainingTerm);
            output.collect(new Text(docTerm.getKey()), docTerm.getValue());
        }
        reporter.setStatus("Combined " + text.toString());
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void configure(JobConf entries) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
