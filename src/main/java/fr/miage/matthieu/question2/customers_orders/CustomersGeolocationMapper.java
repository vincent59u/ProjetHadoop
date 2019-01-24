package fr.miage.matthieu.question2.customers_orders;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class CustomersGeolocationMapper
 *
 * Simple lecture du fichier customerGeolocation-r-00000
 * [customer_id => latitude, longitude]
 */
public class CustomersGeolocationMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");
        try {
            context.write(new Text(tokens[0]), new Text("LAT_LONG~" + tokens[1] + "/" + tokens[2]));
        }catch (ArrayIndexOutOfBoundsException e){
            //System.out.println("Pas de géolocation pour le customer");
        }
    }
}
