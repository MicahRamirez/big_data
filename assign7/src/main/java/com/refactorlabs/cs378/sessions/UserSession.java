
package com.refactorlabs.cs378.sessions;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UserSession extends Configured implements Tool {

    /**
     * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() functio
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {

        private static final Integer INITIAL_COUNT = 1;

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";


        /**
         * Local variable "word" will contain a word identified in the input.
         * The Hadoop Text object is mutable, so we can reuse the same object and
         * simply reset its value as data for each word output.
         */
        private Text word = new Text();
        private static final String[] Type = {"change", "click", "contact_form_status", "edit", "share", "show", "submit", "visit", "null"};
        private static final EventType[] enumTypes = {EventType.CHANGE, EventType.CLICK, EventType.CONTACT_FORM_STATUS, EventType.EDIT, EventType.SHARE, EventType.SHOW, EventType.SUBMIT, EventType.VISIT};

        private static final String[] subType = {"CONTACT_FORM_TYPE", "ALTERNATIVE", "CONTACT_BANNER", "CONTACT_BUTTON","DEALER_PHONE", "FEATURES", "GET_DIRECTIONS","SHOW_MORE_BADGES", "TEST_DRIVE_LINK",
                "VEHICLE_HISTORY","FORM_ERROR", "FORM_SUCCESS", "CONTACT_FORM", "MARKET_REPORT","BADGE_DETAIL", "PHOTO_MODAL","BADGES"};
        private static final EventSubtype[] subTypeEnum = {EventSubtype.CONTACT_FORM_TYPE, EventSubtype.ALTERNATIVE, EventSubtype.CONTACT_BANNER, EventSubtype.CONTACT_BUTTON, EventSubtype.DEALER_PHONE, EventSubtype.FEATURES, EventSubtype.GET_DIRECTIONS,
                EventSubtype.SHOW_MORE_BADGES, EventSubtype.TEST_DRIVE_LINK, EventSubtype.VEHICLE_HISTORY, EventSubtype.FORM_ERROR, EventSubtype.FORM_SUCCESS, EventSubtype.CONTACT_FORM, EventSubtype.MARKET_REPORT, EventSubtype.BADGE_DETAIL, EventSubtype.PHOTO_MODAL, EventSubtype.BADGES};
        private static final String[] cond = {"New", "Used","CPO"};
        private static final Condition[] condEnum = {Condition.New, Condition.Used, Condition.CPO};
        private static final String[] body = {"Chassis", "Convertible", "Coupe", "Hatchback", "Minivan", "Pickup", "SUV", "Sedan", "Van", "Wagon"};
        private static final BodyStyle[] bodyStyle = {BodyStyle.Chassis, BodyStyle.Convertible, BodyStyle.Coupe, BodyStyle.Hatchback, BodyStyle.Minivan, BodyStyle.Pickup, BodyStyle.SUV, BodyStyle.Sedan, BodyStyle.Van, BodyStyle.Wagon};
        private static final String[] cab = {"regular", "extended", "crew"};
        private static final CabStyle[] cabStyle = {CabStyle.Regular, CabStyle.Extended, CabStyle.Crew};

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] input = value.toString().split("\t");
            Event event = new Event();
            String userID = input[0];

            int index = 1;
            //1
            evenTypeAndSub(event, input[index].toLowerCase());
            index++;
            //2
            event.setPage(input[index]);
            index++;
            //3
            event.setReferrer(input[index]);
            index++;
            //4
            event.setReferringDomain(input[index]);
            index++;
            //5
            event.setEventTime(input[index]);
            index++;
            //6
            event.setCity(input[index]);
            index++;
            //7
            event.setRegion(input[index]);
            index++;
            //8
            event.setVin(input[index]);
            index++;
            event.setCondition(detCond(input[index].toLowerCase()));
            index++;
            event.setYear(Integer.parseInt(input[index]));
            index++;
            event.setMake(input[index]);
            index++;
            event.setModel(input[index]);
            index++;
            event.setTrim(input[index]);
            index++;
            event.setBodyStyle(detBodyStyle(input[index]));
            index++;
            event.setSubtrim(input[index]);
            index++;
            event.setCabStyle(detCabStyle(input[index].toLowerCase()));
            index++;
            event.setPrice((Float.parseFloat(input[index])));
            index++;
            event.setMileage(Integer.parseInt(input[index]));
            index++;
            event.setMpg(Integer.parseInt(input[index]));
            index++;
            event.setExteriorColor(input[index]);
            index++;
            event.setInteriorColor(input[index]);
            index++;
            event.setEngineDisplacement(input[index]);
            index++;
            event.setEngine(input[index]);
            index++;
            event.setTransmission(input[index]);
            index++;
            event.setDriveType(input[index]);
            index++;
            event.setFuel(input[index]);
            index++;
            event.setImageCount(Integer.parseInt(input[index]));
            index++;
            event.setFreeCarfaxReport(input[index].equals("t") ? true : false);
            index++;
            event.setCarfaxOneOwner(input[index].equals("t") ? true : false);
            index++;
            event.setCpo(input[index].equals("t") ? true : false);
            List<String> featureList = new ArrayList<String>();
            index++;
            String[] partial = input[index].split(":");
            for(String item: partial) {
                featureList.add(item);
            }
            Collections.sort(featureList);
            List<CharSequence> chn = new ArrayList<CharSequence>(featureList);
            event.setFeatures(chn);


            Session.Builder builder = Session.newBuilder();
            builder.setUserId(userID);
            List<Event> eventList = new ArrayList<Event>();
            eventList.add(event);
            builder.setEvents(eventList);
            word.set(userID);
            context.write(word, new AvroValue(builder.build()));
            context.getCounter(MAPPER_COUNTER_GROUP, "Output Words").increment(1L);


        }

        private static void evenTypeAndSub(Event event, String input){
            if(input.equals("visit_market_report_listing")){
                event.setEventType(EventType.VISIT);
                event.setEventSubtype(EventSubtype.MARKET_REPORT);
            }else{
                String[] splitPhrase = input.split("\\s+");
                for(int i = 0; i < enumTypes.length; i++){
                    if(Type[i].contains(splitPhrase[0])){
                        event.setEventType(enumTypes[i]);
                    }
                }
                event.setEventSubtype(subTypeEnum[0]);
                for(int i = 1; i < splitPhrase.length; i++){
                    for(int j = 0; j < subType.length; j++){
                        if(subType[j].contains(splitPhrase[i])) {
                            event.setEventSubtype(subTypeEnum[j]);
                            break;
                        }
                    }
                }
            }
        }

        private static Condition detCond(String input){
            Condition found = condEnum[0];
            for(int i = 0; i < cond.length; i++){
                if(input.equals(cond[i])){
                    found = condEnum[i];
                }
            }
            return found;
        }

        private static BodyStyle detBodyStyle(String input){
            BodyStyle found = bodyStyle[0];
            for(int i = 0; i < body.length; i++){
                if(input.equals(body[i])){
                    found = bodyStyle[i];
                }
            }
            return found;
        }

        private static CabStyle detCabStyle(String input){
            CabStyle found = cabStyle[0];
            for(int i = 0; i < cabStyle.length;i++){
                if(input.equals(cab[i])){
                    found = cabStyle[i];
                }
            }
            return found;
        }
    }


    /**
     * The Reduce class for word statistics.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word statistics example.
     */
    public static class ReduceClass extends Reducer<Text, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

        /**
         * Counter group for the reducer.  Individual counters are grouped for the reducer.
         */
        private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

        @Override
        public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
                throws IOException, InterruptedException {
            long[] sum = new long[3];
            context.getCounter(REDUCER_COUNTER_GROUP, "Input Words").increment(1L);
            Session.Builder builder = Session.newBuilder();
            // Output the sums for now, to check the values
            int numEvent = 0;
            List<Event> comb = new ArrayList<Event>();
            for(AvroValue<Session> session: values){
                for(Event items : session.datum().getEvents()){
                    comb.add(items);
                }
                numEvent++;
            }

            builder.setEvents(comb);
            builder.setUserId(key.toString());
                context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Session>(builder.build()));
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountD <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "UserSession");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

//        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
//        AvroJob.setOutputKeySchema(job, Session.getClassSchema());
//        MultipleInputs.addInputPath(job, new Path(args[0]), AvroKeyValueInputFormat.class, SessionMapper.class );
//        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, VinImpressionMapper.class);

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(UserSession.class);
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MapClass.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        //job.setOutputValueClass(Session.class);
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
        int res = ToolRunner.run(new Configuration(), new UserSession(), args);
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