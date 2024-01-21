import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;

public class CountMapred {
  public static abstract class CountMapper
      extends Mapper<Object, Text, Text, IntWritable> {
    // 实现这个方法以实现自定义策略
    abstract public String toKey(Log log);

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      try {
        Log log = Log.parseLog(value.toString()); // 解析日志
        String key1 = toKey(log); // 调用自定义策略得到感兴趣的信息
        if (key1 != null) { // 判空
          context.write(new Text(key1), new IntWritable(1));
        }
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class CountReducer
      extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int total = 0;
      for (IntWritable i: values) { // 求和
        total += i.get();
      }
      context.write(key, new IntWritable(total));
    }
  }

  // 封装 main 函数
  public static void bindJob(Job job, Class<? extends CountMapper> mapper,
                             String input, String output) throws IOException {
    job.setMapperClass(mapper);
    job.setCombinerClass(CountReducer.class); // reducer的实现同时也能作为combiner
    job.setReducerClass(CountReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
  }
}
