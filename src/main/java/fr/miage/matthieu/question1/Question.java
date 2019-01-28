package fr.miage.matthieu.question1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Class Question qui étudie les délais de livraison par rapport à la note de satisfaction
 *
 * Fichiers : olist_products_dataset.csv, olist_order_items_dataset.csv
 */
public class Question {

    public static void main(String[] args) throws Exception
    {
        if (args.length != 2) {
            System.err.println("Usage: Question <input path> <input path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Catégorie des produits les plus vendus");
        job.setJarByClass(fr.miage.matthieu.question4.Question.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, fr.miage.matthieu.question1.ProductMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, fr.miage.matthieu.question1.OrderItemsMapper.class);

        Path outputPath = new Path("./output/question1");
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        job.setReducerClass(QuestionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
