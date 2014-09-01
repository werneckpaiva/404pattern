package com.globo.error404.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globo.error404.type.UrlRequest;

public class Filter404Mapper extends Mapper<LongWritable, Text, Text, UrlRequest> {

    protected static Logger _log = LoggerFactory.getLogger(Filter404Mapper.class);

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        UrlRequest urlRequest = UrlRequest.getInstanceFromLogEntry(line);
        // Is 404?
        if (urlRequest.getCode() == null || !urlRequest.getCode().equals(404)){
            return;
        }
        String[] parts = urlRequest.getRequest().split("/");
        Text keyPart;
        for (String part : parts){
            if (part.length() == 0) continue;
            keyPart = new Text(part);
            context.write(keyPart, urlRequest);
        }
    }
}
