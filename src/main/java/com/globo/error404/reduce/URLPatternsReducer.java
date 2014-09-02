package com.globo.error404.reduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globo.error404.type.UrlPart;
import com.globo.error404.type.UrlRequest;

public class URLPatternsReducer extends Reducer<UrlPart, UrlRequest, UrlRequest, UrlPart> {

    protected static Logger _log = LoggerFactory.getLogger(URLPatternsReducer.class);

    public void reduce(UrlPart key, Iterable<UrlRequest> values, Context context) throws IOException, InterruptedException {

        Set<UrlRequest> urlsSet = new HashSet<UrlRequest>();
        for (UrlRequest urlRequest : values) {
            urlsSet.add((UrlRequest)urlRequest.clone());
        }

        if (urlsSet.size() <= 1) return;

        for (UrlRequest urlRequest : urlsSet){
//            if (urlRequest.getRequest().equals("/jogos/noticia/2012/08/blockbusters-para-bombar-seu-console-por-menos-de-r-60")){
//                _log.info(key.toString());
//            }
//            if (key.getPart().equals("artigos")){
//                _log.info(urlRequest.toString());
//            }
            context.write(urlRequest, key);
        }
    }

}
