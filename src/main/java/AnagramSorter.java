
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Anagram sorter
 */
public class AnagramSorter {

    /**
     * Mapper for collecting all anagrams
     */
    public static class MRAnagramSorterMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * Map method for collecting all anagrams
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            char[] valueArray = value.toString().toCharArray();
            Arrays.sort(valueArray);
            context.write(new Text(new String(valueArray)), value);
        }
    }

    /**
     *
     * Combiner for creating groups of anagrams
     */
    public static class MRAnagramSorterCombiner extends Reducer<Text, Text, Text, Text> {

        /**
         * Reduce method for creating groups of anagrams
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> allAnagrams = new ArrayList<String>();
            for (Text value : values) {
                allAnagrams.add(value.toString());
            }
            context.write(new Text(), new Text(StringUtils.join(" ", allAnagrams)));
        }
    }

    /**
     * Reducer for output-ing groups of anagrams sorted in descending order of size
     */
    public static class MRAnagramSorterReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * Comparator class for comparing the lengths of anagram groups
         */
        private static class AnagramGroupLengthCompartor implements Comparator<String> {

            /**
             * Compares the length of two groups of anagrams
             *
             * @param group1
             * @param group2
             * @return comparison result: 0, 1, or -1
             */
            public int compare(String group1, String group2) {
                String[] anagramsGroup1 = group1.trim().split(" ");
                String[] anagramsGroup2 = group2.trim().split(" ");
                return Integer.compare(anagramsGroup1.length, anagramsGroup2.length);
            }
        }

        /**
         * Reduce method for output-ing groups of anagrams sorted in descending order of size
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> listOfAnagramGroups = new ArrayList<String>();
            for (Text value : values) {
                listOfAnagramGroups.add(value.toString());
            }
            Collections.sort(listOfAnagramGroups, Collections.reverseOrder(new AnagramGroupLengthCompartor()));
            context.write(key, new Text(StringUtils.join("\n", listOfAnagramGroups)));
        }
    }

    /**
     * Main method
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration configuration = new Configuration();
        Job anagramSorterJob = Job.getInstance(configuration, "anagramSorter");

        anagramSorterJob.setJarByClass(AnagramSorter.class);

        anagramSorterJob.setOutputKeyClass(Text.class);
        anagramSorterJob.setOutputValueClass(Text.class);

        anagramSorterJob.setMapOutputKeyClass(Text.class);
        anagramSorterJob.setMapOutputValueClass(Text.class);

        anagramSorterJob.setMapperClass(MRAnagramSorterMapper.class);
        anagramSorterJob.setCombinerClass(MRAnagramSorterCombiner.class);
        anagramSorterJob.setReducerClass(MRAnagramSorterReducer.class);

        anagramSorterJob.setInputFormatClass(TextInputFormat.class);
        anagramSorterJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(anagramSorterJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(anagramSorterJob, new Path(args[1]));

        anagramSorterJob.waitForCompletion(true);
    }
}
