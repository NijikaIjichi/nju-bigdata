import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class MaxShip {
  public static class MyMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] fields = value.toString().split("\\|");
      context.write(new Text(fields[5]), new Text(fields[0] + "#" + fields[7]));
    }
  }

  public static class MyReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      int maxShip = Integer.MIN_VALUE;
      ArrayList<Pair<String, Integer>> maxShips = new ArrayList<>();
      for (Text t: values) {
        String[] fields = t.toString().split("#");
        int currShip = Integer.parseInt(fields[1]);
        if (currShip > maxShip) {
          maxShip = currShip;
          maxShips.clear();
          maxShips.add(new Pair<>(fields[0], currShip));
        } else if (currShip == maxShip) {
          maxShips.add(new Pair<>(fields[0], currShip));
        }
      }
      for (Pair<String, Integer> p: maxShips) {
        context.write(new Text(String.format("%s\t%s\t%d", p.getFirst(), key, p.getSecond())),
            NullWritable.get());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MaxShip");
    job.setJarByClass(MaxShip.class);
    job.setMapperClass(MaxShip.MyMapper.class);
    job.setReducerClass(MaxShip.MyReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
