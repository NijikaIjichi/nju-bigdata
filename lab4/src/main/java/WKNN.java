import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class WKNN extends KNN {
  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    conf.set("k", args[0]);
    conf.set("train", args[1]);
    conf.setBoolean("weighted", true);
    Job job = setupJob(conf, WKNN.class, "WKNN", new Path(args[2]), new Path(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
