package com.refactorlabs.cs378.assign7;

import com.refactorlabs.cs378.sessions.*;
import com.refactorlabs.cs378.sessions.Event;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * Created by alpha on 3/28/15.
 */
public class ReduceSideJoin extends Configured implements Tool {


    public static class SessionMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>>{

        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context) throws IOException, InterruptedException{

            Text word = new Text();
            HashMap<String, VinImpressionCounts.Builder> vinMapping = new HashMap<>();
            //for each event if the event has a vin Map it to an Impression Count
            for(Event items: value.datum().getEvents()){
                VinImpressionCounts.Builder builder = new VinImpressionCounts().newBuilder();

                String currentVin = items.getVin().toString();
                if(!vinMapping.containsKey(currentVin)){
                    if(items.getEventType().toString().equals("SHARE") && items.getEventSubtype().toString().equals("MARKET_REPORT")) {
                        builder.setShareMarketReport(1L);
                    }
                    if(items.getEventType().equals(EventType.SUBMIT) && items.getEventSubtype().equals(EventSubtype.CONTACT_FORM)){
                        builder.setSubmitContactForm(1L);
                    }
                    builder.setUniqueUser(1L);
                    Map<CharSequence, Long> clickEvent = new HashMap<>();
                    if(items.getEventType().equals(EventType.CLICK)){
                        clickEvent.put(items.getEventSubtype().toString(), 1L);
                    }
                    builder.setClicks(clickEvent);
                    vinMapping.put(currentVin, builder);
                }else{
                    VinImpressionCounts.Builder curVinObj = vinMapping.get(currentVin);
                    if(items.getEventType().equals(EventType.SHARE) && items.getEventSubtype().equals(EventSubtype.MARKET_REPORT)) {
                        curVinObj.setShareMarketReport(1L);
                    }
                    if(items.getEventType().equals(EventType.SUBMIT) && items.getEventSubtype().toString().contains("CONTACT_FORM")){
                        curVinObj.setSubmitContactForm(1L);
                    }
                    if(items.getEventType().toString().equals("CLICK")){
                        if(curVinObj.getClicks() != null && !curVinObj.getClicks().containsKey(items.getEventSubtype().toString())){
                            Map<CharSequence, Long> temp = curVinObj.getClicks();
                            curVinObj.getClicks().put(items.getEventSubtype().toString(), 1L);
                            curVinObj.setClicks(temp);
                            vinMapping.put(currentVin, curVinObj);
                        }
                    }
                }
            }

            //For each VIN Write its' vinImpression obj
            for(String vin: vinMapping.keySet()){
                word.set(vin);
                context.write(word, new AvroValue(vinMapping.get(vin).build()));
            }
        }


    }
    public static class VinImpressionMapper extends Mapper<LongWritable, Text , Text, AvroValue<VinImpressionCounts>> {

        @Override
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()){
                VinImpressionCounts.Builder builder = new VinImpressionCounts().newBuilder();
                String curLine = tokenizer.nextToken();
                String[] split = curLine.split(",");
                Text vin = new Text();

                if(split.length > 2) {
                    vin.set(split[0]);
                    try{
                        builder.setUniqueUserVdpView(Long.parseLong(split[2]));
                    } catch(NumberFormatException exc) {

                    }
                    context.write(vin, new AvroValue<>(builder.build()));
                }
            }
        }
    }

    public static class ReduceSideJoiner extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>>{

        @Override
        public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context) throws IOException, InterruptedException{
            VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
            Map<CharSequence, Long> eventMap = new HashMap<>();
            List<VinImpressionCounts> test = new ArrayList<>();
            for(AvroValue<VinImpressionCounts> item: values ){
                test.add(VinImpressionCounts.newBuilder(item.datum()).build());
            }

            for(VinImpressionCounts sum :test ){
                //Keep a sum for
                builder.setShareMarketReport(builder.getShareMarketReport() + sum.getShareMarketReport());
                builder.setSubmitContactForm(builder.getSubmitContactForm() + sum.getSubmitContactForm());
                builder.setUniqueUser(builder.getUniqueUser() + sum.getUniqueUser());
                builder.setUniqueUserVdpView(builder.getUniqueUserVdpView() + sum.getUniqueUserVdpView());
                Map<CharSequence, Long> curVinImpressionMap = sum.getClicks();
                if(curVinImpressionMap != null){
                    for(CharSequence keys: curVinImpressionMap.keySet()) {
                        if (!eventMap.containsKey(keys)) {
                            eventMap.put(keys, curVinImpressionMap.get(keys));
                        } else {
                            eventMap.put(keys, eventMap.get(keys) + curVinImpressionMap.get(keys));
                        }
                    }
                    builder.setClicks(eventMap);
                }
            }


            if(builder.getUniqueUser() > 0){
                context.write(key, new AvroValue(builder.build()) );
            }

        }



    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ReduceSideJoin");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "ReduceSideJoin");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        //Specifying Map
        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());
        String[] argSplits = args[0].split(",");
        MultipleInputs.addInputPath(job, new Path(argSplits[0]), AvroKeyValueInputFormat.class, SessionMapper.class);
        MultipleInputs.addInputPath(job, new Path(argSplits[1]), TextInputFormat.class, VinImpressionMapper.class);

        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(ReduceSideJoin.class);
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Reduce
        job.setReducerClass(ReduceSideJoiner.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        String[] inputPaths = appArgs[0].split(",");
        for(String inputPath: inputPaths){
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        printClassPath();
        int res = ToolRunner.run(new Configuration(), new ReduceSideJoin(), args);
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
