package com.refactorlabs.cs378.assign8;

import com.refactorlabs.cs378.sessions.Event;
import com.refactorlabs.cs378.sessions.EventType;
import com.refactorlabs.cs378.sessions.Session;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * Created by alpha on 3/31/15.
 */
public class FilteringMultipleOutputs extends Configured implements Tool {


    public static class MapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<Session>> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        private AvroMultipleOutputs multipleOutputs;
        private static final String SUBMITTER = "Submitter Counts";
        private static final String SHARER = "Sharer Counts";
        private static final String CLICKER = "Clicker Counts";
        private static final String SHOWER = "Shower Counts";
        private static final String VISITOR = "Visitor Counts";
        private static final String OTHER = "Other Counts";

        /**
         * Local variable "word" will contain the word identified in the input.
         * The Hadoop Text object is mutable, so we can reuse the same object and
         * simply reset its value as each word in the input is encountered.
         */
        private Text word = new Text();


        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {
            setup(context);
            if (value.datum().getEvents().size() <= 1000){
                HashSet<EventType> eventSet = new HashSet<>();
                for (Event item : value.datum().getEvents()) {
                    eventSet.add(item.getEventType());
                }
                context.getCounter(SUBMITTER, "Started");
                context.getCounter(SHARER, "Started");
                context.getCounter(CLICKER, "Started");
                context.getCounter(SHOWER, "Started");
                context.getCounter(VISITOR, "Started");
                context.getCounter(OTHER, "Started");


                if (eventSet.contains(EventType.CHANGE) || eventSet.contains(EventType.CONTACT_FORM_STATUS) || eventSet.contains(EventType.EDIT) || eventSet.contains(EventType.SUBMIT)) {
                    multipleOutputs.write("sessionType", key, value, "Submitter");
                    context.getCounter(SUBMITTER, "Submitter").increment(1L);
                } else if (eventSet.contains(EventType.SHARE)) {
                    multipleOutputs.write("sessionType", key, value, "Sharer");
                    context.getCounter(SHARER, "Sharer").increment(1L);
                } else if (eventSet.contains(EventType.CLICK) && !(eventSet.contains(EventType.SHARE))) {
                    multipleOutputs.write("sessionType", key, value, "Clicker");
                    context.getCounter(CLICKER, "Clicker").increment(1L);
                } else if (!(eventSet.contains(EventType.CLICK)) && eventSet.contains(EventType.SHOW)) {
                    multipleOutputs.write("sessionType", key, value, "Shower");
                    context.getCounter(SHOWER, "Shower").increment(1L);
                } else if (eventSet.contains(EventType.VISIT)) {
                    multipleOutputs.write("sessionType", key, value, "Visitor");
                    context.getCounter(VISITOR, "Visitor").increment(1L);
                } else {
                    multipleOutputs.write("sessionType", key, value, "Other");
                    context.getCounter(OTHER, "Other").increment(1L);
                }
            }

            context.getCounter(SUBMITTER, "Ended");
            context.getCounter(SHARER, "Ended");
            context.getCounter(CLICKER, "Ended");
            context.getCounter(SHOWER, "Ended");
            context.getCounter(VISITOR, "Ended");
            context.getCounter(OTHER, "Ended");
            cleanup(context);
        }




        public void setup(Mapper.Context context) {
            multipleOutputs = new AvroMultipleOutputs(context);
        }

        public void cleanup(Context context) throws InterruptedException, IOException{
            multipleOutputs.close();
        }

    }




    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountD <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "FilteringMultipleOutputs");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(FilteringMultipleOutputs.class);
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
        job.setInputFormatClass(AvroKeyValueInputFormat.class);

        job.setMapperClass(MapClass.class);

        AvroMultipleOutputs.addNamedOutput(job, "sessionType", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
        //DID NOT SET THIS ON FIRST RUN
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());
        job.setNumReduceTasks(0);

        // Specify Output Format
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());
        String[] inputPaths = appArgs[0].split(",");
        for(String inputPath: inputPaths){
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));
        job.waitForCompletion(true);
        return 0;
    }


    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        printClassPath();
        int res = ToolRunner.run(new Configuration(), new FilteringMultipleOutputs(), args);
        System.exit(res);
    }

    private static void printClassPath() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader) cl).getURLs();
        System.out.println("classpath BEGIN");
        for (URL url : urls) {
            System.out.println(url.getFile());
        }
        System.out.println("classpath END");
    }
}
