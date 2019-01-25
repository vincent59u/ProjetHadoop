package fr.miage.matthieu.question1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");
        try {
            context.write(new Text(tokens[0]), new Text("PRODUCT_NAME~" + tokens[1]));
        }catch(ArrayIndexOutOfBoundsException e){
            System.out.println("Erreur dans le nombre de colonne");
        }
    }
}
