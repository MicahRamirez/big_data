package com.refactorlabs.cs378.assign2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.lang.Exception;import java.lang.InterruptedException;import java.lang.Long;import java.lang.Override;import java.lang.String;import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Program that calculates statistics of words found in a given dataset.
 *
 * @author Xavier Micah Ramirez EID: xmr73 UTCS:xramirez
 */
public class WordStatistics {

    /**
     * Each count output from the map() function is "1", so to minimize small
     * object creation we can use a constant for this output value/object.
     */
    public final static LongWritable ONE = new LongWritable(1L);

    /**
     * The Map class for word count.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() function for the word statistics example.
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, LongArrayWritable> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        /**
         * Local variable "word" will contain the word identified in the input.
         * The Hadoop Text object is mutable, so we can reuse the same object and
         * simply reset its value as each word in the input is encountered.
         */
        private Text word = new Text();


        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

            String[] search = {"[", ",", ".", "!", ";", "--", "?",  ":", "\"", "]", "_"};
            String[] replace = {" [", "", "", "", "", " ", "", "", "", "]",""};

                    HashMap<String, Long> wordCount = new HashMap<String, Long>();

            while (tokenizer.hasMoreTokens()) {

                //Exchange punctuation specified by search[] and replace[]
                String[] currentTokens = StringUtils.replaceEach(tokenizer.nextToken(),search, replace).toLowerCase().split("\\s+");
                //The tokens are either in the dictionary or not
                for(String currentToken: currentTokens){
                    if(!wordCount.containsKey(currentToken)){
                        wordCount.put(currentToken, 1L);
                    }else{
                        wordCount.put(currentToken, wordCount.get(currentToken) + 1);
                    }
                }
            }
            //Ok I have gone through the paragraph, emit the appropriate values via LongArrayWritable, the String
            //will need to be wrapped in (Text)
            Set<String> setOfWords = wordCount.keySet();
            for(String keyOfSet: setOfWords){
                LongWritable sum = new LongWritable(wordCount.get(keyOfSet));
                LongWritable sOfSquares = new LongWritable(wordCount.get(keyOfSet) * wordCount.get(keyOfSet));
                Writable[] data = { ONE, sum, sOfSquares};
                LongArrayWritable emit = new LongArrayWritable();
                emit.set(data);
                word.set(keyOfSet);
                context.write(word, emit);
                context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            }
            }
        }

        /**
         * Combiner Class
         * Consolidates the Long Array Writables of matching keys by summing of each of the values, num_occurences_para, num occurences
         * per para, the square of the num occurences.
         */
        public static class CombinerClass extends Reducer<Text, LongArrayWritable, Text, LongArrayWritable>{

            public static final String COMBINER_COUNTER_GROUP = "Combiner Counts";

            public void combiner(Text key, java.lang.Iterable<LongArrayWritable> values, Context context) throws IOException, InterruptedException{

                LongArrayWritable emit = new LongArrayWritable();
                emit.sum(values);
                context.write(key, emit);
                context.getCounter(COMBINER_COUNTER_GROUP, "Words Combined").increment(1L);
            }

        }

        /**
         * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
         * This class defines the reduce() function for the word count example.
         */
        public static class ReduceClass extends Reducer<Text, LongArrayWritable, Text, DoubleArrayWritable> {

            /**
             * Counter group for the reducer.  Individual counters are grouped for the reducer.
             */
            private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

            @Override
            public void reduce(Text key, java.lang.Iterable<LongArrayWritable> values, Context context)
                    throws IOException, InterruptedException {

                double[] result = new double[3];
                context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

                // Sum up the counts for the current word, specified in object "key".
                for (LongArrayWritable set : values) {
                    long[] group = set.getValueArray();
                    result[0] += (double)group[0];
                    result[1] += group[1];
                    result[2] += group[2];
                }

                //Calculating Mean in Result[1] and Std Dev Result[2]
                result[1] = result[1]/result[0];
                result[2] = (result[2]/result[0]) - (result[1] * result[1]);
                //Wrap vars in a writable obj
                DoubleArrayWritable emit = new DoubleArrayWritable();
                Writable numOcurrences = new DoubleWritable(result[0]);
                Writable mean = new DoubleWritable(result[1]);
                Writable std = new DoubleWritable(result[2]);
                Writable[] data = {numOcurrences,mean, std};

                // Emit the total count for the word.
                emit.set(data);
                context.write(key, emit);
            }
        }

        /**
         * Class to hold an array of LongWritables
         */
        public static class LongArrayWritable extends ArrayWritable {

            public LongArrayWritable() {
                super(LongWritable.class);
            }

            public long[] getValueArray() {
                Writable[] wValues = get();
                long[] values = new long[wValues.length];
                for (int i = 0; i < values.length; i++) {
                    values[i] = ((LongWritable) wValues[i]).get();
                }
                return values;
            }

            public LongArrayWritable sum(java.lang.Iterable<LongArrayWritable> items){
                long[] sums = new long[3];
                for(LongArrayWritable item: items){
                    long[] lawArr = item.getValueArray();
                    sums[0] += lawArr[0];
                    sums[1] += lawArr[1];
                    sums[2] += lawArr[2];
                }
                LongArrayWritable res = new LongArrayWritable();
                Writable[] temp = {new LongWritable(sums[0]), new LongWritable((sums[1])), new LongWritable(sums[2])};
                res.set(temp);
                return res;
            }
        }

        /**
         * Class to hold an array of DoubleWritables
         */
        public static class DoubleArrayWritable extends ArrayWritable{

            public DoubleArrayWritable(){
                super(DoubleWritable.class);
            }

            public double[] getValueArray() {
                Writable[] wDubValues = get();
                double[] val = new double[wDubValues.length];
                for( int i = 0; i < val.length; i++) {
                    val[i] = ((DoubleWritable)wDubValues[i]).get();
                }
                return val;
            }

            @Override
            public String toString(){

                double[] items = this.getValueArray();
                StringBuilder sb = new StringBuilder();
                for(int i = 0; i < items.length; i++){
                    if(i == items.length - 1){
                        sb.append(items[i]);
                    }else{
                        sb.append(items[i]);
                        sb.append(", ");
                    }
                }
                return sb.toString();
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

            Job job = new Job(conf, "WordStatistics");
            // Identify the JAR file to replicate to all machines.
            job.setJarByClass(WordStatistics.class);

            // Set the output key and value types (for map and reduce).
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongArrayWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleArrayWritable.class);

            // Set the map, combiner, and reduce classes.
            job.setMapperClass(MapClass.class);
            job.setReducerClass(ReduceClass.class);
            job.setCombinerClass(CombinerClass.class);

            // Set the input and output file formats.
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // Grab the input file and output directory from the command line.
            FileInputFormat.addInputPath(job, new Path(appArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

            // Initiate the map-reduce job, and wait for completion.
            job.waitForCompletion(true);
        }
    }