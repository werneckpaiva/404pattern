package com.globo.error404;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globo.error404.map.Filter404Mapper;
import com.globo.error404.map.Filter404Mapper2;
import com.globo.error404.reduce.URLPatternsReducer;
import com.globo.error404.reduce.URLPatternsReducer2;
import com.globo.error404.type.UrlRequest;

public class PatternDiscoveryApp {

    protected static Logger _log = LoggerFactory .getLogger(PatternDiscoveryApp.class);

    private Configuration conf;

    private Job job1Analyze;
    private Job job2Analyze;

    private String inputFolder;

    private String outputFolder;
    
    private final String OUTPUT = "data/_output";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage HelloApp <inputPath> <outpuPath>");
            System.exit(1);
        }
        String inputFolder = args[0];
        String outputFolder = args[1];
        PatternDiscoveryApp app = new PatternDiscoveryApp(inputFolder,
                outputFolder);
        try {
            app.runJob();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(2);
        }
    }

    public PatternDiscoveryApp(String inputFolder, String outputFolder) {
        this.inputFolder = inputFolder;
        this.outputFolder = outputFolder;
        this.conf = new Configuration();
    }

    private void runJob() throws Exception {

        _log.info("Removing output folder");
        FileSystem fs = FileSystem.get(this.conf);
        fs.delete(new Path(this.outputFolder), true);
        fs.delete(new Path(OUTPUT), true);

        _log.info("Configuring job 1...");
        configureJob1();

        _log.info("Running 1 ...");
        Boolean ok = job1Analyze.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job 1 failed");
        }
        
        _log.info("Configuring job 2...");
        configureJob2();

        _log.info("Running 2 ...");
        ok = job2Analyze.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job 2 failed");
        }
        _log.info("Done");
    }

    private void configureJob1() throws IOException {
        job1Analyze = new Job(conf, "keys");
        job1Analyze.setJarByClass(PatternDiscoveryApp.class);

        FileInputFormat.addInputPath(job1Analyze, new Path(inputFolder));
        FileOutputFormat.setOutputPath(job1Analyze, new Path(OUTPUT));

        job1Analyze.setMapperClass(Filter404Mapper.class);
        job1Analyze.setMapOutputKeyClass(Text.class);
        job1Analyze.setMapOutputValueClass(UrlRequest.class);

        job1Analyze.setReducerClass(URLPatternsReducer.class);
        job1Analyze.setOutputKeyClass(Text.class);
        job1Analyze.setOutputValueClass(Text.class);
    }

    private void configureJob2() throws IOException {
        job2Analyze = new Job(conf, "pattenrs");
        job2Analyze.setJarByClass(PatternDiscoveryApp.class);

        FileInputFormat.addInputPath(job2Analyze, new Path(OUTPUT));
        FileOutputFormat.setOutputPath(job2Analyze, new Path(outputFolder));

        job2Analyze.setMapperClass(Filter404Mapper2.class);
        job2Analyze.setMapOutputKeyClass(Text.class);
        job2Analyze.setMapOutputValueClass(Text.class);

        job2Analyze.setReducerClass(URLPatternsReducer2.class);
        job2Analyze.setOutputKeyClass(Text.class);
        job2Analyze.setOutputValueClass(Text.class);
    }
    
}
