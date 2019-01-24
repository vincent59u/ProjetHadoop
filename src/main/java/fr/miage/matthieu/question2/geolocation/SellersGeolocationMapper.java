package fr.miage.matthieu.question2.geolocation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class SellersGeolocationMapper
 *
 * Associe pour chaque zip_code le seller_id
 * [zip_code => seller_id]
 */
public class SellersGeolocationMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");
        context.write(new Text(tokens[1]), new Text("SELLER_ID~" + tokens[0].replace("\"", "")));
    }
}