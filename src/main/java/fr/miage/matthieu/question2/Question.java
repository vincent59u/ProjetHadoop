package fr.miage.matthieu.question2;

import fr.miage.matthieu.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Class Question qui calcul la moyenne des frais de ports en fonction de la distance de livraison
 * @link http://unmeshasreeveni.blogspot.com/2014/04/chaining-jobs-in-hadoop-mapreduce.html
 *
 * Fichiers : olist_customers_dataset.csv, olist_sellers_dataset.csv, olist_geolocation_dataset.csv, olist_orders_dataset.csv, olist_order_items_dataset.csv
 * Utilisation de fichier intermédiaire
 */
public class Question {

    //Déclaration des différents point d'entrée dans le HDFS Hadoop
    private static final Path OUTPUT_PATH = new Path("./output/question2");
    private static final Path TMP_OUTPUT_PATH = new Path("./output/question2_tmp");
    private static final Path DATA = new Path("/user/Matthieu");

    public static void main(String[] args) throws Exception
    {
        if (args.length != 5) {
            System.err.println("Usage: Question <input path> <input path> <input path> <input path> <input path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        /**
         * PREMIER JOB
         */
        /*
        Job job1 = Job.getInstance(conf, "Partie géolocation - Moyenne des frais de ports en fonction de la distance de livraison");
        job1.setJarByClass(fr.miage.matthieu.question2.Question.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, fr.miage.matthieu.question2.geolocation.CustomersGeolocationMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, fr.miage.matthieu.question2.geolocation.SellersGeolocationMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, fr.miage.matthieu.question2.geolocation.GeoMapper.class);

        FileOutputFormat.setOutputPath(job1, TMP_OUTPUT_PATH);
        TMP_OUTPUT_PATH.getFileSystem(conf).delete(TMP_OUTPUT_PATH,true);

        job1.setReducerClass(fr.miage.matthieu.question2.geolocation.GeolocationReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleOutputs.addNamedOutput(job1, "customersGeolocation", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "sellersGeolocation", TextOutputFormat.class, Text.class, Text.class);

        if (job1.waitForCompletion(true)){
            //On bouge les fichiers générés dans le dossier data lorsque le job1 est terminé
            Utils.moveFiles(TMP_OUTPUT_PATH, DATA, conf, "customersGeolocation-r-00000");
            Utils.moveFiles(TMP_OUTPUT_PATH, DATA, conf, "sellersGeolocation-r-00000");

            /**
             * DEUXIEME JOB
             */
        /*
            Job job2 = Job.getInstance(conf, "Partie commande customer - Moyenne des frais de ports en fonction de la distance de livraison");
            job2.setJarByClass(fr.miage.matthieu.question2.Question.class);
            job2.getConfiguration().set("mapreduce.output.basename", "customer_order_location");

            MultipleInputs.addInputPath(job2, new Path(DATA.toString() + "/customersGeolocation-r-00000"), TextInputFormat.class,  fr.miage.matthieu.question2.customers_orders.CustomersGeolocationMapper.class);
            MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, fr.miage.matthieu.question2.customers_orders.OrdersMapper.class);

            TMP_OUTPUT_PATH.getFileSystem(conf).delete(TMP_OUTPUT_PATH,true);
            FileOutputFormat.setOutputPath(job2, TMP_OUTPUT_PATH);

            job2.setReducerClass(fr.miage.matthieu.question2.customers_orders.CustomersOrdersReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            if (job2.waitForCompletion(true)){
                //On bouge les fichiers générés dans le dossier data lorsque le job2 est terminé
                Utils.moveFiles(TMP_OUTPUT_PATH, DATA, conf, "customer_order_location-r-00000");

                /**
                 * TROISIEME JOB
                 */
        /*
                Job job3 = Job.getInstance(conf, "Partie commande seller - Moyenne des frais de ports en fonction de la distance de livraison");
                job3.setJarByClass(fr.miage.matthieu.question2.Question.class);
                job3.getConfiguration().set("mapreduce.output.basename", "seller_order_location");

                MultipleInputs.addInputPath(job3, new Path(DATA.toString() + "/sellersGeolocation-r-00000"), TextInputFormat.class,  fr.miage.matthieu.question2.sellers_orders_items.SellersGeolocationMapper.class);
                MultipleInputs.addInputPath(job3, new Path(args[4]), TextInputFormat.class, fr.miage.matthieu.question2.sellers_orders_items.OrdersItemsMapper.class);

                TMP_OUTPUT_PATH.getFileSystem(conf).delete(TMP_OUTPUT_PATH,true);
                FileOutputFormat.setOutputPath(job3, TMP_OUTPUT_PATH);

                job3.setReducerClass(fr.miage.matthieu.question2.sellers_orders_items.SellersOrdersItemsReducer.class);
                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(Text.class);

                if (job3.waitForCompletion(true)){
                    //On bouge les fichiers générés dans le dossier data lorsque le job3 est terminé
                    Utils.moveFiles(TMP_OUTPUT_PATH, DATA, conf, "seller_order_location-r-00000");

                    /**
                     * QUATRIEME JOB
                     */
                    Job job4 = Job.getInstance(conf, "Partie distance freight value - Moyenne des frais de ports en fonction de la distance de livraison");
                    job4.setJarByClass(fr.miage.matthieu.question2.Question.class);

                    MultipleInputs.addInputPath(job4, new Path(DATA.toString() + "/customer_order_location-r-00000"), TextInputFormat.class,  fr.miage.matthieu.question2.distance_freigth.CustomersOrdersMapper.class);
                    MultipleInputs.addInputPath(job4, new Path(DATA.toString() + "/seller_order_location-r-00000"), TextInputFormat.class, fr.miage.matthieu.question2.distance_freigth.SellersOrdersMapper.class);

                    OUTPUT_PATH.getFileSystem(conf).delete(OUTPUT_PATH,true);
                    FileOutputFormat.setOutputPath(job4, OUTPUT_PATH);

                    job4.setReducerClass(fr.miage.matthieu.question2.distance_freigth.DistanceFreigthReducer.class);
                    job4.setOutputKeyClass(Text.class);
                    job4.setOutputValueClass(Text.class);


                    if (job4.waitForCompletion(true)){
                        //On supprime le dossier temporaire dans le dossier /output du HDFS
                        TMP_OUTPUT_PATH.getFileSystem(conf).delete(TMP_OUTPUT_PATH, true);
                        System.exit(0);
                    }else{
                        System.exit(1);
                    }
                /*}else{
                    System.exit(1);
                }
            }else{
                System.exit(1);
            }
        }else{
            System.exit(1);
        }*/
    }
}
