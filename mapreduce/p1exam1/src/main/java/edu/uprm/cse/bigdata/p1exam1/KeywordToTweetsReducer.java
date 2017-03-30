package edu.uprm.cse.bigdata.p1exam1;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by omar on 3/30/17.
 */
public class KeywordToTweetsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> out = new ArrayList<String>();

        for (Text value : values) {
            out.add(value.toString());
        }

        context.write(key, new Text(StringUtils.join(out, ", ")));
    }
}