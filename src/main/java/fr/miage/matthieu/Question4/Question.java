package fr.miage.matthieu.question4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Class Question qui permet de connaitre les tranches de volume par produit.
 *
 * Fichier : olist_products_dataset.csv
 */
public class Question {

    public static void main(String[] args) throws Exception
    {
        if (args.length != 1) {
            System.err.println("Usage: Question <input path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Volume des produits");
        job.setJarByClass(Question.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path("./output/question4");
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        job.setMapperClass(QuestionMapper.class);
        job.setReducerClass(QuestionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
