import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;

public class MaxShip2 {

  public static class MyPair extends GenericPair<Text, IntWritable> {
    public MyPair(Text text, IntWritable intWritable) {
      super(text, intWritable);
    }

    public MyPair() {
      this(new Text(), new IntWritable());
    }
  }

  public static class MyMapper extends Mapper<Object, Text, MyPair, Text> {

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] fields = value.toString().split("\\|");
      context.write(new MyPair(new Text(fields[5]),
          new IntWritable(-Integer.parseInt(fields[7]))),
          new Text(fields[0]));
    }
  }

  public static class MyPartitioner extends Partitioner<MyPair, Text> {
    private final HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<>();

    @Override
    public int getPartition(MyPair myPair, Text text, int i) {
      return hashPartitioner.getPartition(myPair.getKey(), text, i);
    }
  }

  public static class MyReducer extends Reducer<MyPair, Text, Text, NullWritable> {
    private String curr;

    @Override
    public void setup(Context ctx) {
      curr = "";
    }

    @Override
    public void reduce(MyPair key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String k = key.getKey().toString();
      int v = key.getValue().get();
      if (!curr.equals(k)) {
        curr = k;
        for (Text t: values) {
          context.write(new Text(String.format("%s\t%s\t%d", t.toString(), k, -v)),
              NullWritable.get());
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MaxShip2");
    job.setJarByClass(MaxShip2.class);
    job.setMapperClass(MaxShip2.MyMapper.class);
    job.setPartitionerClass(MaxShip2.MyPartitioner.class);
    job.setReducerClass(MaxShip2.MyReducer.class);
    job.setMapOutputKeyClass(MaxShip2.MyPair.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
