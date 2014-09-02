package com.globo.error404.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globo.error404.type.UrlPart;
import com.globo.error404.type.UrlRequest;

public class URLPatternsReducer2 extends Reducer<UrlRequest, UrlPart, Text, UrlRequest> {

    protected static Logger _log = LoggerFactory.getLogger(URLPatternsReducer2.class);

    public void reduce(UrlRequest key, Iterable<UrlPart> values, Context context) throws IOException, InterruptedException {

        Map<Integer, String> fixedParts = new HashMap<Integer, String>();
        for (UrlPart part : values){
            fixedParts.put(part.getPosition(), part.getPart());
        }
        String[] urlParts = key.getUrlParts();
        for (int i=0; i<urlParts.length; i++){
            if (urlParts[i].equals("")) continue;
            if (!urlParts[i].equals("") && !fixedParts.containsKey(i)){
                urlParts[i]="*";
            }
        }
        String pattern = StringUtils.join(urlParts, '/');
//        _log.info("pattern: {}", pattern);
        context.write(new Text(pattern), key);
    }

}
