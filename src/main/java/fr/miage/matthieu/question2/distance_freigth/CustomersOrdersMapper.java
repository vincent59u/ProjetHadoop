package fr.miage.matthieu.question2.distance_freigth;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class CustomersOrdersMapper
 *
 * Associe pour chaque commande différentes valeurs
 * [order_id => latitude, longitude]
 */
public class CustomersOrdersMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");
        try {
            context.write(new Text(tokens[1]), new Text("CUSTOMER~" + tokens[2] //Latitude
                    + "/" + tokens[3])); //Longitude
        }catch(ArrayIndexOutOfBoundsException e){
            //System.out.println("Erreur dans le nombre d'attribut");
        }
    }
}
