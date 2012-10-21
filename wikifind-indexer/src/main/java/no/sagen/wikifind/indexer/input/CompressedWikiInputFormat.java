package no.sagen.wikifind.indexer.input;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class CompressedWikiInputFormat extends KeyValueTextInputFormat {


    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(job);
        List<InputSplit> splits = new ArrayList<InputSplit>();
        for(FileStatus file : listStatus(job)){
            BZip2Codec codec = (BZip2Codec) codecFactory.getCodec(file.getPath());
            FileSystem fs = file.getPath().getFileSystem(job);

            CompressionInputStream inputStream = codec.createInputStream(fs.open(file.getPath()));
            BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, file.getLen());
            int pos = 0;
            int skippedBytes;
            while((skippedBytes = PageReader.readUntilEndOfPage(inputStream, null)) != -1 && splits.size() < numSplits){
                splits.add(new FileSplit(file.getPath(), pos, pos + skippedBytes, blockLocations[getBlockIndex(blockLocations, pos)].getHosts()));
                pos += skippedBytes;
            }
        }
        return splits.toArray(new InputSplit[splits.size()]);
    }


    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return new CompressedWikiPageRecordReader((FileSplit)split, job, reporter);
    }
}
