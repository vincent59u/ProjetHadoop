package fr.miage.matthieu.question5;

import fr.miage.matthieu.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * Class QuestionMapperCentroid
 *
 * Associe pour chaque cluster les coordonnées GPS des villes contenu dans celui-ci
 * [cluster_id, lat, long]
 */
public class QuestionMapperCentroid extends Mapper<LongWritable, Text, IntWritable, Text> {

    private static List<Double[]> centroids;

    @Override
    public void setup(Context context) throws IOException
    {
        centroids = Utils.readCentroids(context.getCacheFiles()[0].toString());
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String ligne = value.toString();
        String[] tokens = ligne.split(",");
        if (tokens.length == 4) {
            String latitude = tokens[2];
            String longitude = tokens[3];

            if (!latitude.trim().equals("") && !longitude.trim().equals("")) {
                double latValue = Double.parseDouble(latitude);
                double longValue = Double.parseDouble(longitude);
                double minDistance = Double.MAX_VALUE;
                int indexCluster = 0;

                //Cette formule calcul la distance entre les centroid et la valeur courante
                for (int i = 0; i < centroids.size(); i++) {
                    double distance = Math.sqrt(Math.pow(centroids.get(i)[0] - latValue, 2) + Math.pow(centroids.get(i)[1] - longValue, 2));
                    if (distance < minDistance) {
                        indexCluster = i;
                        minDistance = distance;
                    }
                }

                context.write(new IntWritable(indexCluster), new Text(latitude + "," + longitude));
            }
        }
    }
}
