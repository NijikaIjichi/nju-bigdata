import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.util.Objects;

public class UniqueMapred {
  // <Text, Text> 的pair
  public static class TextPair extends GenericPair<Text, Text> {
    public TextPair(Text text, Text text2) {
      super(text, text2);
    }

    public TextPair(String s1, String s2) {
      this(new Text(s1), new Text(s2));
    }

    public TextPair() {
      this(new Text(), new Text());
    }
  }
  public static abstract class UniqueMapper
      extends Mapper<Object, Text, TextPair, NullWritable> {

    //  实现这 2 个方法以实现自定义策略
    abstract public String toKey1(Log log);
    abstract public String toKey2(Log log);

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      try {
        Log log = Log.parseLog(value.toString()); // 解析日志
        String key1 = toKey1(log), key2 = toKey2(log); // 获取感兴趣的信息
        if (key1 != null && key2 != null) { // 判空
          context.write(new TextPair(key1, key2), NullWritable.get());
        }
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class UniqueCombiner
      extends Reducer<TextPair, NullWritable, TextPair, NullWritable> {
    // 将重复的键合并
    @Override
    protected void reduce(TextPair key, Iterable<NullWritable> values, Context context)
        throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }
  }

  public static class UniquePartitioner extends Partitioner<TextPair, NullWritable> {
    private final HashPartitioner<Text, NullWritable> hashPartitioner = new HashPartitioner<>();

    // 确保具有相同key1的被分到同一个 reducer
    @Override
    public int getPartition(TextPair textPair, NullWritable nullWritable, int i) {
      return hashPartitioner.getPartition(textPair.getFirst(), nullWritable, i);
    }
  }

  public static class UniqueReducer
      extends Reducer<TextPair, NullWritable, Text, IntWritable> {
    private String lastK1, lastK2;
    private int count;

    //初始化
    @Override
    protected void setup(Context context) {
      lastK1 = null;
      lastK2 = null;
      count = 0;
    }

    @Override
    public void reduce(TextPair key, Iterable<NullWritable> values, Context context)
        throws IOException, InterruptedException {
      //遍历去重
      String k1 = key.getFirst().toString(), k2 = key.getSecond().toString();
      if (!Objects.equals(k1, lastK1)) {
        if (lastK1 != null) {
          context.write(new Text(lastK1), new IntWritable(count));
        }
        lastK1 = k1;
        lastK2 = null;
        count = 0;
      }
      if (!Objects.equals(k2, lastK2)) {
        lastK2 = k2;
        count += 1;
      }
    }

    // 输出（可能的)最后一组信息
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      if (lastK1 != null) {
        context.write(new Text(lastK1), new IntWritable(count));
      }
    }
  }

  public static void bindJob(Job job, Class<? extends UniqueMapper> mapper,
                             String input, String output) throws IOException {
    job.setMapperClass(mapper);
    job.setCombinerClass(UniqueCombiner.class);
    job.setPartitionerClass(UniquePartitioner.class);
    job.setReducerClass(UniqueReducer.class);
    job.setMapOutputKeyClass(TextPair.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
  }
}
