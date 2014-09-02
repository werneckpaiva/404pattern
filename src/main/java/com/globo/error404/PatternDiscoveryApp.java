package com.globo.error404;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globo.error404.map.Filter404Mapper;
import com.globo.error404.map.Filter404Mapper2;
import com.globo.error404.reduce.URLPatternsReducer;
import com.globo.error404.reduce.URLPatternsReducer2;
import com.globo.error404.type.UrlPart;
import com.globo.error404.type.UrlRequest;
import com.mongodb.DB;
import com.mongodb.MongoClient;

public class PatternDiscoveryApp {

    protected static Logger _log = LoggerFactory .getLogger(PatternDiscoveryApp.class);

    private Configuration conf;

    private Job job1;
    private Job job2;

    private String inputFolder;

    private String outputFolder;
    
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

        _log.info("Configuring job 1...");
        configureJob1();

        _log.info("Running 1 ...");
        Boolean ok = job1.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job 1 failed");
        }

        _log.info("Configuring job 2...");
        configureJob2();

        _log.info("Running 2 ...");
        ok = job2.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job 2 failed");
        }

        _log.info("Output job ...");
        outputJob();

        _log.info("Done");
    }

    private void configureJob1() throws IOException {
        job1 = new Job(conf, "job1");
        job1.setJarByClass(PatternDiscoveryApp.class);

        job1.setMapperClass(Filter404Mapper.class);
        job1.setMapOutputKeyClass(UrlPart.class);
        job1.setMapOutputValueClass(UrlRequest.class);

        job1.setReducerClass(URLPatternsReducer.class);
        job1.setOutputKeyClass(UrlRequest.class);
        job1.setOutputValueClass(UrlPart.class);

        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(inputFolder));
        FileOutputFormat.setOutputPath(job1, new Path(outputFolder + Path.SEPARATOR + "partial"));
    }

    private void configureJob2() throws IOException {
        job2 = new Job(conf, "job2");
        job2.setJarByClass(PatternDiscoveryApp.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setMapperClass(Filter404Mapper2.class);
        job2.setMapOutputKeyClass(UrlRequest.class);
        job2.setMapOutputValueClass(UrlPart.class);

        job2.setReducerClass(URLPatternsReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(UrlRequest.class);

        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(outputFolder + Path.SEPARATOR + "partial/part*"));
        FileOutputFormat.setOutputPath(job2, new Path(outputFolder + Path.SEPARATOR + "final"));
    }


    private void outputJob() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fss = fs.listStatus(new Path(this.outputFolder + Path.SEPARATOR + "final"));


        MongoClient mongoClient = new MongoClient( "localhost" );
        DB db = mongoClient.getDB( "techtudo" );

        // For each file
        for (FileStatus status : fss) {
            Path path = status.getPath();
            if (path.getName().indexOf("part") < 0) continue;

            _log.debug("file: {}", path.getName());

            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
            Text pattern = new Text();
            UrlRequest request = new UrlRequest();
            while (reader.next(pattern, request)) {
                _log.debug("{}", pattern);
            }
            reader.close();
        }
    }
}
