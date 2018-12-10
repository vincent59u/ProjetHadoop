package fr.miage.matthieu.question3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderReviewMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");

        try {
            context.write(new Text(tokens[1]), new Text(checkReview(tokens[2])+"~REVIEW"));
        } catch (ArrayIndexOutOfBoundsException ex){
            return;
        }
    }

    private String checkReview(String review)
    {
        if (review.equals("0") || review.equals("1")
                || review.equals("2") || review.equals("3")
                || review.equals("4") || review.equals("5")){
            return review;
        } else {
            return "NA";
        }
    }
}
