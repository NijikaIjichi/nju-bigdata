import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Sorter {
    public static class SorterMapper extends Mapper<Object, Text, FloatWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // text should be output of InvertedIndex. for example [我们] 1.0 @#$%$#@#$
            String[] buffer = value.toString().split("[\\s,]+");
            context.write(new FloatWritable(Float.parseFloat(buffer[1])), new Text(buffer[0]));
        }
    }

    public static class SorterReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
        @Override
        public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values) {
                context.write(v, key);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sorter");
        job.setJarByClass(Sorter.class);
        job.setMapperClass(Sorter.SorterMapper.class);
        job.setReducerClass(Sorter.SorterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
