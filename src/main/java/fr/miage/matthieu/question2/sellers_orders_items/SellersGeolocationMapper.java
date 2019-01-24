package fr.miage.matthieu.question2.sellers_orders_items;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class SellersGeolocationMapper
 *
 * Simple lecture du fichier sellerGeolocation-r-00000
 * [seller_id => latitude, longitude]
 */
public class SellersGeolocationMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");
        try {
            context.write(new Text(tokens[0]), new Text("LAT_LONG~" + tokens[1] + "/" + tokens[2]));
        }catch (ArrayIndexOutOfBoundsException e){
            System.out.println("Pas de géolocation pour le seller");
        }
    }
}
