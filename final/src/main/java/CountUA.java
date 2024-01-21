import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class CountUA {
  public static class CountMapper extends CountMapred.CountMapper {
    @Override
    public String toKey(Log log) {
      return log.getAgent();
    }
  }

  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "CountUA");
    job.setJarByClass(CountUA.class);
    CountMapred.bindJob(job, CountMapper.class, args[0], args[1]);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
