package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by omar on 3/30/17.
 */
public class KeywordToTweetsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();
        List<String> toFind = Arrays.asList("MAGA", "Dictator", "Impeach", "Drain", "Swamp", "Change");

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweet = status.getText().toUpperCase();

            for (String word : toFind) {
                if (tweet.contains(word.toUpperCase())) {
                    context.write(new Text(word), new Text(String.valueOf(status.getId())));
                }
            }
        } catch (TwitterException e) {

        }
    }
}
