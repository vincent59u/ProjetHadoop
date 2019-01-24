package fr.miage.matthieu.question2.geolocation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class GeoMapper
 *
 * Associe les coordonnées GPS pour chaque zip code
 * [zip_code => Latitude, longitude]
 */
public class GeoMapper extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");

        context.write(new Text(tokens[0]), new Text("LAT_LONG~" + tokens[1] + "/" + tokens[2]));
    }
}
