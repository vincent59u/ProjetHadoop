package fr.miage.matthieu.question1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Class QuestionReducer
 *
 * Regroupe le nom de la catégorie produit et le nombre de commande qui concerne cette dernière grâce au product_id.
 * [product_categorie_name => total]
 */
public class QuestionReducer extends Reducer<Text, Text, Text, IntWritable> {

    private HashMap<String, Integer> orderItems;
    private HashMap<String, String> product;
    private HashMap<String, Integer> resultat;

    @Override
    public void setup(Context context)
    {
        this.orderItems = new HashMap<>();
        this.product = new HashMap<>();
        this.resultat = new HashMap<>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
    {
        values.forEach(value -> {
            String[] valueSplit = value.toString().split("~");

            if (valueSplit.length > 1) {
                switch (valueSplit[0]) {
                    case "NUMBER":
                        if (orderItems.containsKey(key.toString())) {
                            orderItems.put(key.toString(), orderItems.get(key.toString()) + Integer.parseInt(valueSplit[1]));
                        } else {
                            orderItems.put(key.toString(), Integer.parseInt(valueSplit[1]));
                        }
                        break;
                    case "PRODUCT_NAME":
                        product.put(key.toString(), valueSplit[1]);
                        break;
                }
            }
        });
    }

    @Override
    public void cleanup(Context context)
    {
        //Regroupement par même catégories de produit (Beaucoup de doublon dans les id
        List<String> keyList = new ArrayList(product.keySet());
        keyList.forEach(key -> {
            if (resultat.containsKey(product.get(key))){
                resultat.put(product.get(key), resultat.get(product.get(key)) + orderItems.get(key));
            }else {
                resultat.put(product.get(key), orderItems.get(key));
            }
        });

        //Ecriture
        Object[] sorted = resultat.entrySet().toArray();
        Arrays.sort(sorted, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Map.Entry<String, Integer>) o2).getValue()
                        .compareTo(((Map.Entry<String, Integer>) o1).getValue());
            }
        });

        for (Object s : sorted) {
            try {
                context.write(new Text(((Map.Entry<String, Integer>) s).getKey()), new IntWritable(((Map.Entry<String, Integer>) s).getValue()));
            } catch (IOException | InterruptedException | NullPointerException e) {
                e.printStackTrace();
            }
        }
    }
}
