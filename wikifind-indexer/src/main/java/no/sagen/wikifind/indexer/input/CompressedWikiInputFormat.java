package no.sagen.wikifind.indexer.input;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class CompressedWikiInputFormat extends FileInputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(job.getConfiguration());
        List<InputSplit> splits = new ArrayList<>();
        List<FileStatus> fileStatuses = listStatus(job);
        for(FileStatus file : fileStatuses){
            BZip2Codec codec = (BZip2Codec) codecFactory.getCodec(file.getPath());
            FileSystem fs = file.getPath().getFileSystem(job.getConfiguration());

            CompressionInputStream inputStream = codec.createInputStream(fs.open(file.getPath()));
            BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, file.getLen());
            int pos = 0;
            int skippedBytes;
            while((skippedBytes = PageReader.readUntilEndOfPage(inputStream, null)) != -1){
                splits.add(new FileSplit(file.getPath(), pos, pos + skippedBytes, blockLocations[getBlockIndex(blockLocations, pos)].getHosts()));
                pos += skippedBytes;
            }
        }
        return splits;
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new CompressedWikiPageRecordReader();
    }
}
