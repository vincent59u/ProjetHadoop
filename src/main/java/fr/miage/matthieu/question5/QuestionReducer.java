package fr.miage.matthieu.question5;

import fr.miage.matthieu.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Class QuestionReducer
 *
 * Permet d'écrire les 3 villes les plus ou moins acheteuses
 * [coordonnees_GPS, Nb_achat]
 */
public class QuestionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private HashMap<String, Integer> resultat;

    @Override
    public void setup(Context context)
    {
        this.resultat = new HashMap<>();
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
    {
        values.forEach(value -> {
            resultat.put(key.toString(), value.get());
        });
    }

    @Override
    public void cleanup(Context context)
    {
        Object[] sorted = Utils.sortByValue(resultat);

        for (int i = 0; i < sorted.length; i++){
            //TOP 3
            if (i < 3) {
                try {
                    context.write(new Text(((Map.Entry<String, Integer>) sorted[i]).getKey()), new IntWritable(((Map.Entry<String, Integer>) sorted[i]).getValue()));
                } catch (IOException | InterruptedException | NullPointerException e) {
                    e.printStackTrace();
                }
            }

            //FLOP 3
            if (i >= sorted.length - 3){
                try {
                    context.write(new Text(((Map.Entry<String, Integer>) sorted[i]).getKey()), new IntWritable(((Map.Entry<String, Integer>) sorted[i]).getValue()));
                } catch (IOException | InterruptedException | NullPointerException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
