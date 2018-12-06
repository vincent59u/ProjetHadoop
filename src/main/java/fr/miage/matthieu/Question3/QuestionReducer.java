package fr.miage.matthieu.question3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class QuestionReducer extends Reducer<Text, Text, IntWritable, DoubleWritable>{

    private String delai, note;
    private int error_count = 0;

    private Map<Integer, Double> delaiNoteSatisfaction;

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
                        delaiNoteSatisfaction.put(clef, (delaiNoteSatisfaction.get(clef) + Double.parseDouble(note)) / 2);
                    } else {
                        delaiNoteSatisfaction.put(clef, Double.parseDouble(note));
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
                context.write(new IntWritable(key), new DoubleWritable(delaiNoteSatisfaction.get(key)));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
