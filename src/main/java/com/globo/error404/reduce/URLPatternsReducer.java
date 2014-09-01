package com.globo.error404.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globo.error404.type.UrlRequest;

public class URLPatternsReducer extends Reducer<Text, UrlRequest, Text, Text> {

    protected static Logger _log = LoggerFactory.getLogger(URLPatternsReducer.class);

    public void reduce(Text key, Iterable<UrlRequest> values, Context context) throws IOException, InterruptedException {

        Map<String, UrlRequest> map = new HashMap<String, UrlRequest>();
        Long count = 0L;
        for (UrlRequest value : values) {
            map.put(value.getRequest(), value);
            count++;
        }
        if (map.size() <= 1) return;
//        if (key.toString().equals("windows-phone.json")){
//            for (String request : map.keySet()){
//                _log.debug("{}", request);
//            }
//        }
        for (String request : map.keySet()){
            context.write(new Text(request), key);
        }
    }

}
