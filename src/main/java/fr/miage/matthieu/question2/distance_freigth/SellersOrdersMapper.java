package fr.miage.matthieu.question2.distance_freigth;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class SellersOrdersMapper
 *
 * Associe pour chaque commande différentes valeurs
 * [order_id => freight_value, latitude, longitude]
 */
public class SellersOrdersMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");
        try {
            context.write(new Text(tokens[1]), new Text("SELLER~" + tokens[2] //Freight_value
                    + "/" + tokens[3] //Latitude
                    + "/" + tokens[4])); //Longitude
        }catch (ArrayIndexOutOfBoundsException e){
            //System.out.println("Erreur dans le nombre d'attribut");
        }
    }
}
