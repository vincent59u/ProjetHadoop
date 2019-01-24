package fr.miage.matthieu.question2.geolocation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class CustomersGeolocationMapper
 *
 * Associe pour chaque zip_code le customer_id
 * [zip_code => customer_id]
 */
public class CustomersGeolocationMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");
        context.write(new Text(tokens[2]), new Text("CUSTOMER_ID~" + tokens[0].replace("\"", "")));
    }
}