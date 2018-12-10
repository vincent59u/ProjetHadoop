package fr.miage.matthieu.question3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text delaiLivraison = new Text();
    private int error_count = 0;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date order_approved_at = formatter.parse(tokens[4]);
            Date order_delivered_customer_date = formatter.parse(tokens[6]);

            //Il faut que la commande soit delivered
            //Et que les Dates soient cohérentes
            if (tokens[2].equals("delivered") && order_approved_at.compareTo(order_delivered_customer_date) < 0) {
                long diff = order_delivered_customer_date.getTime() - order_approved_at.getTime();
                String delai = String.valueOf(TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS));
                delaiLivraison.set(delai+"~ORDER");
            }

            context.write(new Text(tokens[0]), delaiLivraison);
        } catch (ParseException e) {
            //e.printStackTrace();
            error_count++;
            System.out.println("--> Erreur de parsage (" + error_count + ")");
        }
    }
}
