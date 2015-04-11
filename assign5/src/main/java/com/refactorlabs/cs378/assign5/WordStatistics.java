
package com.refactorlabs.cs378.assign5;


import com.google.common.collect.Maps;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * MapReduce program to collect word statistics (per paragraph in the input document), and
 * insert these statistics into Avro objects
 *
 * Removes punctuation and maps all words to lower case.
 *
 * @author Xavier Micah Ramirez and David Franke
 * Modified Dr. Franke's version of WordStatistics to work with Avro objects
 */
public class WordStatistics extends Configured implements Tool {

    /**
     * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() functio
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>> {

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


        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = standardize(value.toString());
            StringTokenizer tokenizer = new StringTokenizer(line);

            context.getCounter(MAPPER_COUNTER_GROUP, "Input Documents").increment(1L);

            Map<String, Integer> wordCountMap = Maps.newHashMap();
            // For each word in the input document, determine the number of times the
            // word occurs.  Keep the current counts in a hash map.
            while (tokenizer.hasMoreTokens()) {
                String nextWord = tokenizer.nextToken();
                Integer count = wordCountMap.get(nextWord);

                if (count == null) {
                    wordCountMap.put(nextWord, INITIAL_COUNT);
                } else {
                    wordCountMap.put(nextWord, count.intValue() + 1);
                }
            }

            // Create the output value for each word, and output the key/value pair.
            for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
                int count = entry.getValue().intValue();
                WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();

                word.set(entry.getKey());
                builder.setDocumentCount(1L);
                builder.setTotalCount(count);
                builder.setSumOfSquares(count * count);
                builder.setMean(0.0);
                builder.setVariance(0.0);
                context.write(word, new AvroValue(builder.build()));
                context.getCounter(MAPPER_COUNTER_GROUP, "Output Words").increment(1L);
            }
        }

        /**
         * Remove punctuation and insert spaces where needed, so the tokenizer will identify words.
         */
        private String standardize(String input) {
            return StringUtils.replaceEach(input,
                    new String[]{".", ",", "\"", "_", "[", ";", "--", ":", "?"},
                    new String[]{" ", " ", " ", " ", " [", " ", " ", " ", " "}).toLowerCase();
        }
    }


    /**
     * The Reduce class for word statistics.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word statistics example.
     */
    public static class ReduceClass extends Reducer<Text, AvroValue<WordStatisticsData>, AvroKey<Pair<CharSequence, WordStatisticsData>>, NullWritable> {

        /**
         * Counter group for the reducer.  Individual counters are grouped for the reducer.
         */
        private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

        @Override
        public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context)
                throws IOException, InterruptedException {
            long[] sum = new long[3];
            for(AvroValue<WordStatisticsData> current: values){
                sum[0]+= current.datum().getDocumentCount();
                sum[1]+= current.datum().getTotalCount();
                sum[2]+= current.datum().getSumOfSquares();
            }

            context.getCounter(REDUCER_COUNTER_GROUP, "Input Words").increment(1L);
            WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
            // Output the sums for now, to check the values
            Writable[] writableArray = new Writable[3];
            double mean = (double)sum[1] / sum[0];
            double variance = ((double)sum[2]/sum[0]) - mean*mean;
            builder.setDocumentCount(sum[0]);
            builder.setTotalCount(sum[1]);
            builder.setSumOfSquares(sum[2]);
            builder.setMean(mean);
            builder.setVariance(variance);
            context.write(new AvroKey<Pair<CharSequence, WordStatisticsData>>(new Pair<CharSequence, WordStatisticsData>(key.toString(), builder.build())), NullWritable.get());
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountD <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "WordStatistics");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatistics.class);
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MapClass.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, WordStatisticsData.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(job,
                Pair.getPairSchema(Schema.create(Schema.Type.STRING), WordStatisticsData.getClassSchema()));
        job.setOutputValueClass(NullWritable.class);

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
        int res = ToolRunner.run(new Configuration(), new WordStatistics(), args);
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