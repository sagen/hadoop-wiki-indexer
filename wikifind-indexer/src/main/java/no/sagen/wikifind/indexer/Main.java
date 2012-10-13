package no.sagen.wikifind.indexer;


import no.sagen.wikifind.indexer.input.CompressedWikiInputFormat;
import no.sagen.wikifind.indexer.mapper.TermsInWikiPageMapper;
import no.sagen.wikifind.indexer.output.TfIdfRiakOutputFormat;
import no.sagen.wikifind.indexer.reducer.WikiPageTFIDFReducer;
import no.sagen.wikifind.indexer.transfer.TermFrequencyInDocument;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

import static org.apache.hadoop.mapred.FileInputFormat.setInputPaths;

public class Main {

    public static final String DOCS_COUNT_FIILE = "docs";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        JobConf jobConf = new JobConf();
        //jobConf.setJarByClass(StreamJob.class);
        setInputPaths(jobConf, new Path("/home/sagen/Downloads/nowiki-20120907-pages-articles.xml.bz2"));

        jobConf.setInputFormat(CompressedWikiInputFormat.class);
        jobConf.setMapperClass(TermsInWikiPageMapper.class);
        jobConf.setOutputFormat(TfIdfRiakOutputFormat.class);
        jobConf.setReducerClass(WikiPageTFIDFReducer.class);
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(TermFrequencyInDocument.class);
        jobConf.set("io.sort.mb", "300");
        jobConf.setNumMapTasks(2);
        jobConf.setNumReduceTasks(2);
        //jobConf.set
        JobClient jobClient = new JobClient(jobConf);
        jobClient.getFs().create(new Path(DOCS_COUNT_FIILE));
        jobClient.submitJob(jobConf).waitForCompletion();
        jobClient.getFs().delete(new Path(DOCS_COUNT_FIILE), true);
        jobClient.getFs().close();
    }
}
