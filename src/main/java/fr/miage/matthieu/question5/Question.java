package fr.miage.matthieu.question5;

import fr.miage.matthieu.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Class Question
 * Permet de faire un K-means pour trouver les villes ou dont faite le plus et moins de commande
 */
public class Question {

    private static Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage : Question <input path> <centroids>");
            System.exit(-1);
        }

        String centroidFileTemp = args[1] + ".tmp";
        Files.write(Paths.get(centroidFileTemp), "".getBytes());

        Utils.replaceFileContent(centroidFileTemp, args[1]);

        boolean isFinish = false;
        int iteration = 0;

        while (!isFinish && iteration < 20){

            /**
             * Appel du premier JOB
             */
            if (!startCentroidJob(args[0], centroidFileTemp)) {
                System.exit(-1);
            }

            if (Utils.checkEqualContent(centroidFileTemp, "./output/question5-centroids/part-r-00000")) {
                isFinish = true;
            } else {
                Utils.replaceFileContent(centroidFileTemp, "./output/question5-centroids/part-r-00000");
            }

            iteration++;
            System.out.println("Itération sur les centroids : " + iteration);
        }

        /**
         * Deuxième JOB
         */
        Job job2 = Job.getInstance(conf, "K-means des villes plus ou moins consomatrices");
        job2.setJarByClass(fr.miage.matthieu.question5.Question.class);

        FileInputFormat.addInputPath(job2, new Path("./output/question5-centroids/part-r-00000"));
        Path outputPath = new Path("./output/question5");
        FileOutputFormat.setOutputPath(job2, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        job2.setMapperClass(QuestionMapper.class);
        job2.setReducerClass(QuestionReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    private static boolean startCentroidJob(String dataFilename, String centroids) throws IOException, ClassNotFoundException, InterruptedException
    {
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job1 = Job.getInstance(conf, "K-means des villes plus ou moins consomatrices");
        job1.setJarByClass(Question.class);

        FileInputFormat.addInputPath(job1, new Path(dataFilename));
        Path outputPath = new Path("./output/question5-centroids");
        FileOutputFormat.setOutputPath(job1, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        //Ajout du fichier temporaire centroids
        job1.addCacheFile(new Path(centroids).toUri());

        job1.setMapperClass(QuestionMapperCentroid.class);
        job1.setReducerClass(QuestionReducerCentroid.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);

        return job1.waitForCompletion(true);
    }
}
