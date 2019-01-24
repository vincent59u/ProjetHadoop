package fr.miage.matthieu.question2.distance_freigth;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Class DistanceFreigthReducer
 *
 * Calcul la distance entre le client et le vendeur et la moyenne des frais de port.
 * [tranche_distance => moyenne_freight_velue]
 */
public class DistanceFreigthReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    //Structure [order_id => cust_lat, cust_long]
    private HashMap<String, List<String>> customers;

    //Structure [order_id => sell_lat, sell_long, freight_value]
    private HashMap<String, List<String>> sellers;

    //Liste des commandes sans doublon
    private Set<String> order;

    private HashMap<String, Integer> increment;

    private HashMap<String, Double> resultat;


    @Override
    public void setup(Context context)
    {
        this.customers = new HashMap<>();
        this.sellers = new HashMap<>();
        this.order = new LinkedHashSet<>();
        this.increment = new HashMap<>();
        this.resultat = new HashMap<>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
    {
        //Ajout de la commande dans le set
        order.add(key.toString());

        values.forEach(value -> {
            String[] valueSplit = value.toString().split("~");

            switch (valueSplit[0]){
                case "CUSTOMER" :
                    String[] cust_arr = valueSplit[1].split("/");

                    //Si la commande est déjà initialisée
                    if (!this.customers.containsKey(key.toString())){
                        this.customers.put(key.toString(), new ArrayList<>(Arrays.asList(cust_arr[0], cust_arr[1])));
                    }
                    break;
                case "SELLER" :
                    String[] sell_arr = valueSplit[1].split("/");

                    //Si la commande est déjà initialisée
                    if (this.sellers.containsKey(key.toString())){
                        this.sellers.get(key.toString()).add(2,
                                Double.toString(
                                Double.parseDouble(sellers.get(key.toString()).get(2))
                                    + Double.parseDouble(sell_arr[0])));

                        this.increment.replace(key.toString(), this.increment.get(key.toString())+1);
                    }else{
                        this.sellers.put(key.toString(), new ArrayList<>(Arrays.asList(sell_arr[1], sell_arr[2], sell_arr[0])));
                        this.increment.put(key.toString(), 1);
                    }
                    break;
                default:
                    System.out.println("Valeur inattendue dans le DistanceFreigthReducer ----> " + value);
                    break;
            }
        });
    }

    @Override
    public void cleanup(Context context)
    {
        //Calcul
        for (String o : order) {
            if (customers.containsKey(o) && sellers.containsKey(o)) {
                int div_euclidienne = (int)distance(Double.parseDouble(customers.get(o).get(0)),
                        Double.parseDouble(customers.get(o).get(1)),
                        Double.parseDouble(sellers.get(o).get(0)),
                        //'K' => Kilomètre
                        Double.parseDouble(sellers.get(o).get(1)), 'K') / 100;

                String key = div_euclidienne + "00-" + (div_euclidienne+1) + "00";
                if (resultat.containsKey(key)){
                    resultat.put(key, resultat.get(key) + Double.parseDouble(sellers.get(o).get(2))/increment.get(o));
                    this.increment.replace(key, this.increment.get(key)+1);
                }else{
                    resultat.put(key, Double.parseDouble(sellers.get(o).get(2))/increment.get(o));
                    this.increment.put(key, 1);
                }
            }
        }

        //Ecriture
        List<String> keyList = new ArrayList(resultat.keySet());
        keyList.sort(Comparator.comparingInt((String t) -> Integer.valueOf(t.split("-")[0])));
        keyList.forEach(key -> {
            try {
                context.write(new Text(key), new DoubleWritable(resultat.get(key)/this.increment.get(key)));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    /*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
    /*::    Calcul de distance avec des lat et long (Stackoverflow)     :*/
    /*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
    /**
     * @link https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude-what-am-i-doi
     */
    private double distance(double lat1, double lon1, double lat2, double lon2, char unit) {
        double theta = lon1 - lon2;
        double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        dist = dist * 60 * 1.1515;
        if (unit == 'K') {
            dist = dist * 1.609344;
        } else if (unit == 'N') {
            dist = dist * 0.8684;
        }
        return dist;
    }

    private double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
    }


    private double rad2deg(double rad) {
        return (rad * 180.0 / Math.PI);
    }
}
