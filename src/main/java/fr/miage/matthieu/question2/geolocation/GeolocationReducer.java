package fr.miage.matthieu.question2.geolocation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;

/**
 * Class GeolocationReducer
 *
 * Crée deux fichiers différents pour customer et seller
 * Associe, pour chaque individu, sa latitude et longitude
 */
public class GeolocationReducer extends Reducer<Text, Text, Text, Text> {

    private HashMap<String, String> sellers, customers, coordonnees;

    @Override
    public void setup(Context context)
    {
        this.sellers = new HashMap<>();
        this.customers = new HashMap<>();
        this.coordonnees = new HashMap<>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
    {
        values.forEach(value -> {
            String[] valueSplit = value.toString().split("~");

            switch (valueSplit[0]) {
                case "CUSTOMER_ID":
                    customers.put(valueSplit[1], key.toString());
                    break;
                case "SELLER_ID":
                    sellers.put(valueSplit[1], key.toString());
                    break;
                case "LAT_LONG":
                    String[] lat_long_array = valueSplit[1].split("/");
                    coordonnees.put(key.toString(), lat_long_array[0] + "," + lat_long_array[1]);
                    break;
                default:
                    System.out.println("Valeur inattendue dans le GeolocationReducer ----> " + value);
                    break;
            }
        });
    }

    @Override
    public void cleanup(Context context)
    {
        MultipleOutputs out = new MultipleOutputs(context);

        for(Map.Entry<String, String> entry : customers.entrySet()) {
            try {
                out.write("customersGeolocation", entry.getKey(), new Text(""+coordonnees.get(entry.getValue())));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        for(Map.Entry<String, String> entry : sellers.entrySet()) {
            try {
                out.write("sellersGeolocation", entry.getKey(), new Text(""+coordonnees.get(entry.getValue())));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
