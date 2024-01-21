import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SumNation {
  public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] fields = value.toString().split("\\|");
      context.write(new Text(fields[3]), new DoubleWritable(Double.parseDouble(fields[5])));
    }
  }

  public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      double total = 0.0;
      for (DoubleWritable d: values) {
        total += d.get();
      }
      context.write(key, new DoubleWritable(total));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "SumNation");
    job.setJarByClass(SumNation.class);
    job.setMapperClass(SumNation.MyMapper.class);
    job.setReducerClass(SumNation.MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
