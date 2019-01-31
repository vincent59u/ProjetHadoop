package fr.miage.matthieu.question5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class QuestionMapper
 *
 * Associe pour chaque coordonnées GPS des centroids le nombre de commande dans les 100km
 * [coordonnées_GPS, Nb_commande]
 */
public class QuestionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");
        context.write(new Text(tokens[1] + "," + tokens[2]), new IntWritable(Integer.parseInt(tokens[3])));
    }
}
