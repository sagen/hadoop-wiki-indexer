package no.sagen.wikifind.indexer.input;

import no.sagen.wikifind.indexer.Main;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.OutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

import static no.sagen.wikifind.indexer.input.PageReader.readUntilEndOfPage;

public class CompressedWikiPageRecordReader implements RecordReader<Text, Text> {

    private FileSplit fileSplit;
    private CompressionInputStream is;
    private int pos;
    private Reporter reporter;
    private final FileSystem fs;
    private long count;
    public CompressedWikiPageRecordReader(FileSplit fileSplit, JobConf job, Reporter reporter) throws IOException {
        this.fileSplit = fileSplit;
        fs = fileSplit.getPath().getFileSystem(job);
        BZip2Codec codec = (BZip2Codec) new CompressionCodecFactory(job).getCodec(this.fileSplit.getPath());
        is = codec.createInputStream(fs.open(fileSplit.getPath()));
        this.reporter = reporter;
    }

    @Override
    public float getProgress() throws IOException {
        return pos / fileSplit.getLength();
    }

    @Override
    public boolean next(Text key, Text value) throws IOException {
        OutputBuffer buf = new OutputBuffer();
        int movedPos;
        if ((movedPos = readUntilEndOfPage(is, buf)) == -1) {
            return false;
        }
        pos += movedPos;
        key.set("");
        value.set(buf.getData());
        count++;
        return true;
    }

    @Override
    public Text createKey() {
        return new Text();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public void close() throws IOException {
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(Main.DOCS_COUNT_FIILE));
        fsDataOutputStream.writeLong(count);
        fsDataOutputStream.close();
        System.out.println("Closed RecordReader");
        is.close();
    }
}
