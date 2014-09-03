package com.globo.error404.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globo.error404.type.UrlPart;
import com.globo.error404.type.UrlRequest;

public class URLPatternsReducer extends Reducer<UrlPart, UrlRequest, UrlRequest, UrlPart> {

    protected static Logger _log = LoggerFactory.getLogger(URLPatternsReducer.class);

    public void reduce(UrlPart key, Iterable<UrlRequest> values, Context context) throws IOException, InterruptedException {

        Boolean hasDifferentURLs = false;
        String firstNormalizedUrl = values.iterator().next().getNormalizedRequest();
        List<UrlRequest> urlRequestList = new ArrayList<UrlRequest>();
        for (UrlRequest urlRequest : values) {
            urlRequestList.add((UrlRequest) urlRequest.clone());
            String normalizedUrl = urlRequest.getNormalizedRequest();
            if (!hasDifferentURLs && !normalizedUrl.equals(firstNormalizedUrl)){
                hasDifferentURLs = true;
            }
        }
        StringBuffer sb = new StringBuffer();
        for (UrlRequest urlRequest : urlRequestList){
            sb.append("[");
            sb.append(urlRequest.getRequest());
            sb.append("] ");
            sb.append(urlRequest.getNormalizedRequest());
            sb.append(", ");
        }
        if (!hasDifferentURLs) {
//            _log.debug("{} - count: {}", key, urlRequestList.size());
            if (sb.toString().indexOf("plantao") >=0 && urlRequestList.size() > 1) _log.debug("{} - {}", key, sb.toString());
            return;
        }
        
        for (UrlRequest urlRequest : urlRequestList){
//            _log.debug("{} - ***** {}", key, urlRequest);
            context.write(urlRequest, key);
        }
    }

}
