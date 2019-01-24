package fr.miage.matthieu.question2.customers_orders;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class OrdersMapper
 *
 * Associe pour chaque customer l'order_id qu'il a effectué
 * [custommer_id => order_id]
 */
public class OrdersMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");
        try {
            if (tokens[2].equals("delivered"))
                context.write(new Text(tokens[1].replace("\"", "")), new Text("ORDER_ID~" + tokens[0].replace("\"", "")));
        }catch (ArrayIndexOutOfBoundsException e){
            System.out.println("Erreur dans l'indice du tableau");
        }
    }
}
