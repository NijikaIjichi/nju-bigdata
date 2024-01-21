import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class InvertedIndex {
  public static class MyMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      String fileName = fileSplit.getPath().getName();
      Text fileNameText = new Text(fileName);
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        context.write(new Text(itr.nextToken()), fileNameText);
      }
    }
  }

  public static class MyReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      HashMap<String, Integer> timesMap = new HashMap<>();
      int times = 0;
      boolean first = true;
      for (Text t: values) {
        String fileName = t.toString();
        timesMap.put(fileName, timesMap.getOrDefault(fileName, 0) + 1);
        times += 1;
      }
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("%.2f", ((double) times) / timesMap.size()));
      for (Map.Entry<String, Integer> entry: timesMap.entrySet()) {
        if (first) {
          sb.append(", ");
          first = false;
        } else {
          sb.append("; ");
        }
        sb.append(entry.getKey()).append(":").append(entry.getValue());
      }
      context.write(new Text("[" + key.toString() + "]"), new Text(sb.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "InvertedIndex");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(InvertedIndex.MyMapper.class);
    job.setReducerClass(InvertedIndex.MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
