package fr.miage.matthieu.question2.sellers_orders_items;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class OrdersItemsMapper
 *
 * Associe pour chaque seller l'order_id qu'il a effectué
 * [seller_id => order_id]
 */
public class OrdersItemsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");
        context.write(new Text(tokens[3].replace("\"", "")),
                new Text("ORDER_ID~" + tokens[0].replace("\"", "") //Order_id
                + "/" + tokens[6])); // Freight_value
    }
}
