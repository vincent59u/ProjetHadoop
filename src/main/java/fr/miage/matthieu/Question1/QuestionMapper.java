package fr.miage.matthieu.Question1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class QuestionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text volume = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        if (key.get() == 0)
            return;

        String ligne = value.toString();
        String[] tokens = ligne.split(",");

        //Données manquantes dans le csv
        if (tokens.length == 9) {
            int product_length_cm = Integer.parseInt(tokens[6]);
            int product_height_cm = Integer.parseInt(tokens[7]);
            int product_width_cm = Integer.parseInt(tokens[8]);

            int vol = product_length_cm * product_height_cm * product_width_cm;

            if (0 < vol && vol < 1999) {
                volume.set("0-1999");
            } else if (2000 < vol && vol < 3999) {
                volume.set("2000-3999");
            } else if (4000 < vol && vol < 5999) {
                volume.set("4000-5999");
            } else if (6000 < vol && vol < 7999) {
                volume.set("6000-7999");
            } else if (8000 < vol && vol < 9999) {
                volume.set("8000-9999");
            } else if (10000 < vol && vol < 11999) {
                volume.set("10000-11999");
            } else if (12000 < vol && vol < 13999) {
                volume.set("12000-13999");
            } else if (14000 < vol && vol < 15999) {
                volume.set("14000-15999");
            } else {
                volume.set("16000-+");
            }

            context.write(volume, new IntWritable(1));
        }
    }
}
