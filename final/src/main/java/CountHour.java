import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class CountHour {
  public static class CountMapper extends CountMapred.CountMapper {
    @Override
    public String toKey(Log log) {
      SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");
      return format.format(log.getTimestamp());
    }
  }

  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "CountHour");
    job.setJarByClass(CountHour.class);
    CountMapred.bindJob(job, CountMapper.class, args[0], args[1]);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
