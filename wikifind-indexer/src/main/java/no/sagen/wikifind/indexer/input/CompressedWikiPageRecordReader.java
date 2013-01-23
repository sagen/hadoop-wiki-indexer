package no.sagen.wikifind.indexer.input;

import no.sagen.wikifind.indexer.Main;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.OutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import static no.sagen.wikifind.indexer.Main.TMP_DOC_COUNT_FILE;
import static no.sagen.wikifind.indexer.input.PageReader.readUntilEndOfPage;

public class CompressedWikiPageRecordReader extends RecordReader<Text, Text> {

    private FileSplit fileSplit;
    private CompressionInputStream is;
    private int pos;
    private FileSystem fs;
    private int count;
    private TaskAttemptContext taskAttemptContext;
    Text currentKey, currentValue;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
       this.fileSplit = (FileSplit) inputSplit;
        this.taskAttemptContext = taskAttemptContext;
        fs = fileSplit.getPath().getFileSystem(taskAttemptContext.getConfiguration());
        BZip2Codec codec = (BZip2Codec) new CompressionCodecFactory(taskAttemptContext.getConfiguration()).getCodec(this.fileSplit.getPath());
        is = codec.createInputStream(fs.open(fileSplit.getPath()));
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        OutputBuffer buf = new OutputBuffer();
        int movedPos;
        if ((movedPos = readUntilEndOfPage(is, buf)) == -1) {
            System.out.println("Read " + count + " wikipedia articles");
            currentKey = null;
            currentValue = null;
            return false;
        }
        pos += movedPos;
        currentKey = new Text("");
        currentValue = new Text(buf.getData());
        count++;
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException {
        return pos / fileSplit.getLength();
    }

    @Override
    public void close() throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(TMP_DOC_COUNT_FILE));
        bw.write(Integer.toString(count));
        bw.close();
        taskAttemptContext.setStatus("Closed RecordReader");
        System.out.println("Pages read: " + count);
        is.close();
    }
}
