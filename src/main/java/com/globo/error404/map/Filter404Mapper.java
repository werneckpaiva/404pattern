package com.globo.error404.map;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globo.error404.type.UrlPart;
import com.globo.error404.type.UrlRequest;

public class Filter404Mapper extends Mapper<LongWritable, Text, UrlPart, UrlRequest> {

    protected static Logger _log = LoggerFactory.getLogger(Filter404Mapper.class);

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        UrlRequest urlRequest = UrlRequest.getInstanceFromLogEntry(line);
        // Is 404?
        if (urlRequest.getCode() == null || !urlRequest.getCode().equals(404)){
            return;
        }
        String[] parts = urlRequest.getUrlParts();
        UrlPart keyPart;
        for (Integer i=0; i<parts.length; i++){
            String part = parts[i];
            if (part.length() == 0) continue;
            if (part.matches("^[0-9]+$")) continue;
            if (part.matches("^[0-9]+\\.html$")) continue;
            keyPart = new UrlPart(i, part);
            context.write(keyPart, urlRequest);
        }
    }
}
