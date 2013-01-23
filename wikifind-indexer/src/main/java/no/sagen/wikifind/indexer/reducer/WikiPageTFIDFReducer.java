package no.sagen.wikifind.indexer.reducer;

import no.sagen.wikifind.indexer.Main;
import no.sagen.wikifind.indexer.transfer.DocumentTerm;
import no.sagen.wikifind.indexer.transfer.TfIdfWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static no.sagen.wikifind.indexer.Main.TMP_DOC_COUNT_FILE;

public class WikiPageTFIDFReducer extends Reducer<Text, DocumentTerm, Text, TfIdfWritable> {

    private long totalNumberOfDocuments;

    @Override
    public void reduce(Text term, Iterable<DocumentTerm> incomingDocumentTerms, Context context) throws IOException, InterruptedException {
        int docsContainingTerm = 0;
        List<DocumentTerm> documentTerms = new ArrayList<>();
        for(DocumentTerm docTerm : incomingDocumentTerms){
            docsContainingTerm++;
            documentTerms.add(new DocumentTerm(docTerm));
        }
        float idf = (float) Math.log(totalNumberOfDocuments / ((double) docsContainingTerm));
        TfIdfWritable out = new TfIdfWritable(idf);
        for(DocumentTerm docTerm : documentTerms){
            out.add(docTerm.getDocId(),((float) (1 + Math.log(docTerm.getTermFrequency())) * idf), docTerm.getInTitle());
        }
        context.write(term, out);
        context.setStatus("Reduced " + term.toString());
    }

    @Override
    public void setup(Context context) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(TMP_DOC_COUNT_FILE));
            totalNumberOfDocuments = Integer.parseInt(br.readLine());
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
