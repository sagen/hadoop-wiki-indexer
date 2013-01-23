package no.sagen.wikifind.indexer;


import no.sagen.wikifind.indexer.input.CompressedWikiInputFormat;
import no.sagen.wikifind.indexer.mapper.IdToTitleMapper;
import no.sagen.wikifind.indexer.mapper.MapperForEuclideanNorm;
import no.sagen.wikifind.indexer.mapper.TermsInWikiPageMapper;
import no.sagen.wikifind.indexer.output.EuclideanNormOutputFormat;
import no.sagen.wikifind.indexer.output.IdToTitleOutputFormat;
import no.sagen.wikifind.indexer.output.TfIdfRiakOutputFormat;
import no.sagen.wikifind.indexer.reducer.IdToTitleReducer;
import no.sagen.wikifind.indexer.reducer.ReducerForEuclideanNorm;
import no.sagen.wikifind.indexer.reducer.WikiPageTFIDFReducer;
import no.sagen.wikifind.indexer.transfer.DocumentTerm;
import no.sagen.wikifind.indexer.transfer.DocumentTermFrequenciesWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;


public class Main extends Configured implements Tool {

    public static final String TMP_DOC_COUNT_FILE = "/tmp/hadoop-wiki-indexer/docs";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);

    }

    private void titles() throws IOException, ClassNotFoundException, InterruptedException {
        Job jobConf = new Job(getConf(), "Title indexer");
        FileInputFormat.addInputPath(jobConf, new Path("/home/sagen/Downloads/nowiki-latest-pages-articles.xml.bz2"));
        jobConf.setInputFormatClass(CompressedWikiInputFormat.class);
        jobConf.setMapperClass(IdToTitleMapper.class);
        jobConf.setMapOutputKeyClass(IntWritable.class);
        jobConf.setMapOutputValueClass(Text.class);
        jobConf.setReducerClass(IdToTitleReducer.class);
        jobConf.setOutputFormatClass(IdToTitleOutputFormat.class);
        jobConf.setNumReduceTasks(1);
        jobConf.setJobName("titles");
        jobConf.waitForCompletion(true);
    }

    private void tfidf() throws IOException, ClassNotFoundException, InterruptedException {
        Job jobConf = new Job(getConf(), "TF-IDF indexer");
        FileInputFormat.addInputPath(jobConf, new Path("/home/sagen/Downloads/nowiki-latest-pages-articles.xml.bz2"));
        jobConf.setInputFormatClass(CompressedWikiInputFormat.class);
        jobConf.setMapperClass(TermsInWikiPageMapper.class);
        jobConf.setReducerClass(WikiPageTFIDFReducer.class);
        jobConf.setOutputFormatClass(TfIdfRiakOutputFormat.class);
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(DocumentTerm.class);
        jobConf.setNumReduceTasks(1);
        jobConf.waitForCompletion(true);
        new File(TMP_DOC_COUNT_FILE).delete();
    }


    private void euclideanLengths() throws IOException, ClassNotFoundException, InterruptedException {
        Job jobConf = new Job(getConf(), "Euclidean norms");
        FileInputFormat.addInputPath(jobConf, new Path("/home/sagen/Downloads/nowiki-latest-pages-articles.xml.bz2"));
        jobConf.setInputFormatClass(CompressedWikiInputFormat.class);
        jobConf.setMapperClass(MapperForEuclideanNorm.class);
        jobConf.setReducerClass(ReducerForEuclideanNorm.class);
        jobConf.setOutputFormatClass(EuclideanNormOutputFormat.class);
        jobConf.setMapOutputKeyClass(IntWritable.class);
        jobConf.setMapOutputValueClass(DocumentTermFrequenciesWritable.class);
        jobConf.setNumReduceTasks(1);
        jobConf.waitForCompletion(true);
        new File(TMP_DOC_COUNT_FILE).delete();
    }


    @Override
    public int run(String[] strings) throws Exception {
        tfidf();
        titles();
        euclideanLengths();
        return 0;
    }
}
