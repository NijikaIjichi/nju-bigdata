import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class UniqueIP {
  public static class UniqueMapper extends UniqueMapred.UniqueMapper {

    @Override
    public String toKey1(Log log) {
      return log.getRequest();
    }

    @Override
    public String toKey2(Log log) {
      return log.getClientIP();
    }
  }

  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "UniqueIP");
    job.setJarByClass(UniqueIP.class);
    UniqueMapred.bindJob(job, UniqueMapper.class, args[0], args[1]);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
