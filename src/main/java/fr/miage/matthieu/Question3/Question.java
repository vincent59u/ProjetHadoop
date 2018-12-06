package fr.miage.matthieu.question3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Class Question qui étudie les délais de livraison par rapport à la note de satisfaction
 */
public class Question {

    public static void main(String[] args) throws Exception
    {
        if (args.length != 2) {
            System.err.println("Usage: Question <input path> <input path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Corrélation délai livraison avec la note de satisfaction");
        job.setJarByClass(fr.miage.matthieu.question3.Question.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, fr.miage.matthieu.question3.OrderMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, fr.miage.matthieu.question3.OrderReviewMapper.class);

        Path outputPath = new Path("./output/question3");
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        job.setReducerClass(QuestionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
