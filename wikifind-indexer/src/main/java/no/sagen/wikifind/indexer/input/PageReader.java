package no.sagen.wikifind.indexer.input;

import org.apache.hadoop.io.OutputBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

public class PageReader {
    static byte[] endPagePattern, startPagePattern;
    static {
        try {
            startPagePattern = "<page>".getBytes("UTF-8");
            endPagePattern = "</page>".getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
           e.printStackTrace();
        }
    }
    static int readUntilEndOfPage(InputStream in, OutputBuffer out) throws IOException {
        int movedPos;
        if((movedPos = moveToStartOfPage(in)) == -1){
            return -1;
        }
        if(out != null){
            out.write(startPagePattern);
        }
        int matchingEndPos = 0;
        int data;
        while((data = in.read()) != -1){
            movedPos++;
            if(out != null){
                out.write(data);
            }
            if(endPagePattern[matchingEndPos] == data){
                if(matchingEndPos == endPagePattern.length - 1){
                    return movedPos;
                }
                matchingEndPos++;
            }else{
                matchingEndPos = 0;
            }
        }
        return -1;
    }

    private static int moveToStartOfPage(InputStream in) throws IOException {
        int data;
        int matchingStartPos = 0;
        int movedPos = 0;
        while((data = in.read()) != -1){
            movedPos++;
            if(startPagePattern[matchingStartPos] == data){
                if(matchingStartPos == startPagePattern.length - 1){
                    return movedPos;
                }
                matchingStartPos++;
            }else{
                matchingStartPos = 0;
            }

        }
        return -1;
    }
}
