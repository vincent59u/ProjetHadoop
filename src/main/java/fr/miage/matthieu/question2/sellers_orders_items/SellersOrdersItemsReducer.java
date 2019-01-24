package fr.miage.matthieu.question2.sellers_orders_items;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Class SellersOrdersItemsReducer
 *
 * Crée un fichier avec chaque commande d'un customer
 * [customer_id => order_id, latitude, longitude]
 */
public class SellersOrdersItemsReducer extends Reducer<Text, Text, Text, Text> {

    private HashMap<String, String> sellers_coordonnees;
    private HashMap<String, List<String>> sellers_orders;

    @Override
    public void setup(Context context)
    {
        this.sellers_coordonnees = new HashMap<>();
        this.sellers_orders = new HashMap<>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
    {
        values.forEach(value -> {
            String[] valueSplit = value.toString().split("~");

            switch (valueSplit[0]){
                case "ORDER_ID" :
                    //Si le seller est contenu dans plusieurs commandes
                    if (this.sellers_orders.containsKey(key.toString())){
                        //On ajoute la nouvelle commande
                        this.sellers_orders.get(key.toString()).add(valueSplit[1].replace("/", ",")  + ",");
                    }else {
                        //Sinon on crée une entrée dans la map
                        this.sellers_orders.put(key.toString(), new ArrayList<>(Arrays.asList(valueSplit[1].replace("/", ",") + ",")));
                    }
                    break;
                case "LAT_LONG" :
                    String[] lat_long_array = valueSplit[1].split("/");
                    this.sellers_coordonnees.put(key.toString(),  lat_long_array[0] + "," + lat_long_array[1]);
                    break;
                default:
                    System.out.println("Valeur inattendue dans le SellersOrdersItemsReducer ----> " + value);
                    break;
            }
        });
    }

    @Override
    public void cleanup(Context context)
    {
        for(Map.Entry<String, List<String>> entry : sellers_orders.entrySet()) {
            String key = entry.getKey();
            List<String> order = entry.getValue();
            for (String o: order) {
                try {
                    context.write(new Text(key), new Text(o + this.sellers_coordonnees.get(key)));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
