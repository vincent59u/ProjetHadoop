package fr.miage.matthieu.question5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Class QuestionReducerCentroid
 *
 * Permet de calculer les centroids de chaque cluster et de calculer le nombre d'achat dans les 100 km
 * [cluster_id, lat, long, Nb_commande_100km]
 */
public class QuestionReducerCentroid extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        double latCentroid = 0.0;
        double longCentroid = 0.0;
        int count = 0;

        Set<Double[]> locationInCentroid = new HashSet<>();
        for (Text value: values) {
            Double[] location = new Double[2];
            String[] latLong = value.toString().split(",");

            latCentroid += Double.parseDouble(latLong[0]);
            location[0] = Double.parseDouble(latLong[0]);

            longCentroid += Double.parseDouble(latLong[1]);
            location[1] = Double.parseDouble(latLong[1]);

            locationInCentroid.add(location);
            count += 1;
        }

        latCentroid = latCentroid/count;
        longCentroid = longCentroid/count;

        int countInHundredKm = 0;
        for (Double[] location : locationInCentroid) {
            if (distanceKm(location[0], location[1], latCentroid, longCentroid) <= 100){
                countInHundredKm += 1;
            }
        }

        context.write(key, new Text(latCentroid + "," + longCentroid + "," + countInHundredKm));
    }

    private double distanceKm(double lat1, double lon1, double lat2, double lon2) {
        double theta = lon1 - lon2;
        double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        dist = dist * 60 * 1.1515 * 1.609344;
        return dist;
    }

    private double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
    }

    private double rad2deg(double rad) {
        return (rad * 180.0 / Math.PI);
    }

}
