import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KNN {
  public static Job setupJob(Configuration conf, Class<? extends KNN> Class, String name,
                             Path input, Path output) throws IOException {
    Job job = Job.getInstance(conf, name);
    job.setJarByClass(Class);
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(Reducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    return job;
  }
  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    conf.set("k", args[0]);
    conf.set("train", args[1]);
    conf.setBoolean("weighted", false);
    Job job = setupJob(conf, KNN.class, "KNN", new Path(args[2]), new Path(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
