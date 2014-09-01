package com.globo.error404.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Filter404Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

    protected static Logger _log = LoggerFactory.getLogger(Filter404Mapper2.class);

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        _log.info(key.toString());
        _log.info(value.toString());
    }
}
