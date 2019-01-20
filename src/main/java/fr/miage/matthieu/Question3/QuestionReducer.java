package fr.miage.matthieu.question3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Class QuestionReducer
 *
 * Regroupe le délai de livraison et la note de satisfaction en fonction de l'order_id.
 * Compte également le nombre de commande par jours de délai de livraison
 * [délai => note_satisfaction, total]
 */
public class QuestionReducer extends Reducer<Text, Text, IntWritable, Text>{

    private String delai, note;
    private int error_count = 0;

    private Map<Integer, Double[]> delaiNoteSatisfaction;

    @Override
    public void setup(Context context) {
        this.delaiNoteSatisfaction = new HashMap<>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
    {
        values.forEach(value -> {
            String[] valueSplit = value.toString().split("~");
            if (valueSplit[1].equals("ORDER")){
                delai = valueSplit[0];
            }else if (valueSplit[1].equals("REVIEW")){
                note = valueSplit[0];
            }

            if (!note.equals("NA")) {
                try {
                    int clef = Integer.parseInt(delai);
                    if (delaiNoteSatisfaction.containsKey(clef)) {
                        Double[] tab = delaiNoteSatisfaction.get(clef);
                        tab[0] = (tab[0] + Double.parseDouble(note)) / 2;
                        tab[1]++;
                        delaiNoteSatisfaction.put(clef, tab);
                    } else {
                        delaiNoteSatisfaction.put(clef, new Double[] {Double.parseDouble(note), 1.0});
                    }
                } catch (Exception ex){
                    //ex.printStackTrace();
                    error_count++;
                    System.out.println("--> Erreur calcul de moyenne (" + error_count + ")");
                }
            }
        });
    }

    @Override
    public void cleanup(Context context){
        List<Integer> keyList = new ArrayList(delaiNoteSatisfaction.keySet());
        keyList.forEach(key -> {
            try {
                Double[] tab = delaiNoteSatisfaction.get(key);
                context.write(new IntWritable(key), new Text(tab[0].toString() + "," + tab[1].intValue()));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
