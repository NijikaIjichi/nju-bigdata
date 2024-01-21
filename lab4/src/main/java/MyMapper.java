import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.PriorityQueue;

public class MyMapper extends Mapper<Object, Text, IntWritable, Text> {
  private ArrayList<Data> trains;
  private int curr_id, k;
  private boolean weighted;

  @Override
  public void setup(Context context)
      throws IOException {
    Configuration conf = context.getConfiguration();
    FSDataInputStream train = FileSystem.get(conf).open(new Path(conf.get("train")));
    BufferedReader reader = new BufferedReader(new InputStreamReader(train));
    String s;
    trains = new ArrayList<>();
    curr_id = 0;
    k = Integer.parseInt(conf.get("k"));
    weighted = conf.getBoolean("weighted", false);
    while ((s = reader.readLine()) != null) {
      trains.add(new Data(s.split(",")));
    }
    reader.close();
  }

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    Data curr = new Data(value.toString().split(","));
    PriorityQueue<Pair> pq = new PriorityQueue<>();
    for (Data data: trains) {
      pq.add(new Pair(curr.distance(data), data));
      while (pq.size() > k) {
        pq.poll();
      }
    }
    context.write(new IntWritable(curr_id), new Text(Pair.mostFrequent(pq, weighted)));
    curr_id += 1;
  }
}