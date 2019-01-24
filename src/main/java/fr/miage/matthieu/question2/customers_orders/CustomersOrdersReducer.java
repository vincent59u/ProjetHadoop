package fr.miage.matthieu.question2.customers_orders;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Class CustomersOrdersReducer
 *
 * Crée un fichier avec chaque commande d'un customer
 * [customer_id => order_id, latitude, longitude]
 */
public class CustomersOrdersReducer extends Reducer<Text, Text, Text, Text> {

    private HashMap<String, List<String>> cust_order;
    private HashMap<String, String> cust_coordonnees;

    @Override
    public void setup(Context context)
    {
        this.cust_order = new HashMap<>();
        this.cust_coordonnees = new HashMap<>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
    {
        values.forEach(value -> {
            String[] valueSplit = value.toString().split("~");

            switch (valueSplit[0]){
                case "ORDER_ID" :
                    //Si on a deja une commande d'un client
                    if (this.cust_order.containsKey(key.toString())){
                        //On ajoute la nouvelle commande à la liste du client
                        this.cust_order.get(key.toString()).add(valueSplit[1]  + ",");
                    }else{
                        //Sinon on crée une nouvelle entrée
                        this.cust_order.put(key.toString(), new ArrayList<>(Arrays.asList(valueSplit[1]  + ",")));
                    }
                    break;
                case "LAT_LONG" :
                    String[] lat_long_array = valueSplit[1].split("/");
                    this.cust_coordonnees.put(key.toString(), lat_long_array[0] + "," + lat_long_array[1]);
                    break;
                default:
                    System.out.println("Valeur inattendue dans le CustomersOrdersReducer ----> " + value);
                    break;
            }
        });
    }

    @Override
    public void cleanup(Context context)
    {
        for(Map.Entry<String, List<String>> entry : cust_order.entrySet()) {
            String key = entry.getKey();
            List<String> order = entry.getValue();
            for (String o: order) {
                try {
                    context.write(new Text(key), new Text(o + this.cust_coordonnees.get(key)));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
