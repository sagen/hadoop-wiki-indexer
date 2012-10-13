package no.sagen.wikifind.indexer.reducer;

import no.sagen.wikifind.indexer.transfer.DocumentTerm;
import no.sagen.wikifind.indexer.transfer.TermFrequencyInDocument;
import no.sagen.wikifind.indexer.transfer.TfIdfWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static no.sagen.wikifind.indexer.Main.DOCS_COUNT_FIILE;

public class WikiPageTFIDFReducer implements Reducer<Text, TermFrequencyInDocument, Text, TfIdfWritable> {

    private long recordCount;

    @Override
    public void reduce(Text term, Iterator<TermFrequencyInDocument> values, OutputCollector<Text, TfIdfWritable> output, Reporter reporter) throws IOException {
        int docsContainingTerm = 0;
        List<DocumentTerm> documentTerms = new ArrayList<>();

        while (values.hasNext()){
            docsContainingTerm++;
            TermFrequencyInDocument termFrequencyInDoc = values.next();
            documentTerms.add(new DocumentTerm(termFrequencyInDoc.getDocId(), termFrequencyInDoc.getFrequence()));
        }
        TfIdfWritable out = new TfIdfWritable(docsContainingTerm);
        for(DocumentTerm doc : documentTerms){
            doc.setDocumentFrequency(docsContainingTerm);
            out.add(doc.getDocId(), doc.getTermFrequency() * Math.log(recordCount / ((double)doc.getDocumentFrequency())));
        }

        output.collect(term, out);
        reporter.setStatus("Reduced " + term.toString());
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void configure(JobConf conf) {
        try {
            FSDataInputStream countInputStream = new JobClient(conf).getFs().open(new Path(DOCS_COUNT_FIILE));
            recordCount = countInputStream.readLong();
            countInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
