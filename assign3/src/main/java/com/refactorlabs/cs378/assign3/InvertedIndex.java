package com.refactorlabs.cs378.assign3;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.lang.Exception;import java.lang.InterruptedException;
import java.lang.Override;import java.lang.String;
import java.util.*;

/**
 * Example MapReduce program that performs an Inverted Index
 *
 * @author Xavier Micah Ramirez
 * UT EID: xmr73
 * UTCS: xramirez
 */
public class InvertedIndex {


    /**
     * The Map class for word count.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() function for the word count example.
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        /**
         * Local variable "word" will contain the word identified in the input.
         * The Hadoop Text object is mutable, so we can reuse the same object and
         * simply reset its value as each word in the input is encountered.
         *
         * The docID is set in the first if loop since the first piece of input will be the verse
         */
        private Text word = new Text();
        private Text docID = new Text();

        private static String[] search = {"[", ",", ".", "!", ";", "--", "?",  ":", "\"", "]", "_", "\t","("};
        private static String[] replace = {" [", "", "", "", "", "", "", "", "", "] ", "", "", ""};

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            //Set docID to the verse
            if(tokenizer.hasMoreTokens()){
                docID.set(tokenizer.nextToken());
            }

            //Use a hashset to determine membership in an input line
            HashSet<String> verseWords = new HashSet<String>();
            while (tokenizer.hasMoreTokens()) {
                String[] currentTokens = StringUtils.replaceEach(tokenizer.nextToken(), search, replace).toLowerCase().split("\\s+");
                //for each token in the split add it to the HashSet
                for(String token: currentTokens){
                    verseWords.add(token);
                }
            }

            //For every item in the hashset emit the word and verse
            for (String verseWord : verseWords) {
                word.set(verseWord);
                context.write(word, docID);
            }

            context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);


        }
    }


    /**
     * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word count example.
     */
    public static class ReduceClass extends Reducer<Text, Text, Text, Text> {

        /**
         * Counter group for the reducer.  Individual counters are grouped for the reducer.
         */
        private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";


        @Override
        public void reduce(Text key, java.lang.Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<String> toBeSorted = new ArrayList<String>();
            //Text items into an ArrayList so I can use Collections.sort
            for(Text item: values){
                toBeSorted.add(item.toString());
            }

            Collections.sort(toBeSorted);

            //Rebuild the string with the proper formatting
            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < toBeSorted.size(); i++){
                sb.append(toBeSorted.get(i));
                if(i != toBeSorted.size() - 1){
                    sb.append(",");
                }
            }
            Text docIDs = new Text();
            docIDs.set(sb.toString());
            key.set(key.toString());
            context.write(key, docIDs);
            context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);
        }




        }







    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = new Job(conf, "InvertedIndex");
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(InvertedIndex.class);

        // Set the output key and value types (for map and reduce).
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the map, combiner, and reduce classes.
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        //job.setCombinerClass(CombinerClass.class);

        // Set the input and output file formats.
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPath(job, new Path(appArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);
    }
}