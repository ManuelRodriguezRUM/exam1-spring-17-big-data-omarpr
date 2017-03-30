package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by omar on 3/30/17.
 */
public class KeywordToTweetsDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: KeywordToTweetsDriver <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(edu.uprm.cse.bigdata.p1exam1.KeywordToTweetsDriver.class);
        job.setJobName("KeywordToTweets");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(edu.uprm.cse.bigdata.p1exam1.KeywordToTweetsMapper.class);
        job.setReducerClass(edu.uprm.cse.bigdata.p1exam1.KeywordToTweetsReducer.class);
        //job.setCombinerClass(class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //System.println("Done!");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
