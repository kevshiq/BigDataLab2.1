package com.example.hadoophomework;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DailyFundReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double allIn = 0.0;
        double allOut = 0.0;

        for (Text val : values) {
            String[] amounts = val.toString().split(",");
            try {
                double in = Double.parseDouble(amounts[0]);
                double out = Double.parseDouble(amounts[1]);
                allIn += in;
                allOut += out;
            } catch (NumberFormatException e) {
                continue;
            }
        }

        result.set(allIn + "," + allOut);
        context.write(key, result);
    }
}