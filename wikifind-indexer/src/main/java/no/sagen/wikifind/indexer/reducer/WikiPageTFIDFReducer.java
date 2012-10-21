package no.sagen.wikifind.indexer.reducer;

import no.sagen.wikifind.indexer.transfer.DocumentTerm;
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

public class WikiPageTFIDFReducer implements Reducer<Text, DocumentTerm, Text, TfIdfWritable> {

    private long totalNumberOfDocuments;

    @Override
    public void reduce(Text term, Iterator<DocumentTerm> incomingDocumentTerms, OutputCollector<Text, TfIdfWritable> output, Reporter reporter) throws IOException {
        int docsContainingTerm = 0;
        List<DocumentTerm> documentTerms = new ArrayList<>();
        while (incomingDocumentTerms.hasNext()){
            docsContainingTerm++;
            documentTerms.add(new DocumentTerm(incomingDocumentTerms.next()));
        }
        float idf = (float) Math.log(totalNumberOfDocuments / ((double) docsContainingTerm));
        TfIdfWritable out = new TfIdfWritable(idf);
        for(DocumentTerm docTerm : documentTerms){
            out.add(docTerm.getDocId(),((float) (1 + Math.log(docTerm.getTermFrequency())) * idf), docTerm.getInTitle());
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
            totalNumberOfDocuments = countInputStream.readInt();
            countInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
