package com.globo.error404.map;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globo.error404.type.UrlPart;
import com.globo.error404.type.UrlRequest;

public class Filter404Mapper2 extends Mapper<UrlRequest, UrlPart, UrlRequest, UrlPart> {

    protected static Logger _log = LoggerFactory.getLogger(Filter404Mapper2.class);

    public void map(UrlRequest request, UrlPart part, Context context)
            throws IOException, InterruptedException {

        context.write(request, part);

    }
}
