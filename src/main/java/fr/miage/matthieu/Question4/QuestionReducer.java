package fr.miage.matthieu.question4;

import java.io.IOException;
import java.util.*;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QuestionReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    private Map<Text, Integer> volumeProductMap;

    @Override
    public void setup(Context context) {
        this.volumeProductMap = new HashMap<>();
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
    {
        int count = StreamSupport.stream(values.spliterator(), false)
                .mapToInt(IntWritable::get)
                .sum();

        volumeProductMap.put(new Text(key), count);
    }

    @Override
    public void cleanup(Context context){
        List<Text> keyList = new ArrayList(volumeProductMap.keySet());
        keyList.sort(Comparator.comparingInt((Text t) -> Integer.valueOf(t.toString().split("-")[0])));
        keyList.forEach(key -> {
            try {
                context.write(key, new IntWritable(volumeProductMap.get(key)));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
