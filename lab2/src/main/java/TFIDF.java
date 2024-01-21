import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

public class TFIDF {

    /**
     * keyout: word_bookname
     * valueout: wordfreq
     */
    public static class TMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object object, Text text, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            HashMap<String, Integer> n = new HashMap<>();
            String bookName = fileSplit.getPath().getName();
            StringTokenizer strtok = new StringTokenizer(text.toString());
            while (strtok.hasMoreTokens()) {
                String s = strtok.nextToken();
                n.put(s, n.getOrDefault(s, 0) + 1);
            }
            for (Map.Entry<String, Integer> entry : n.entrySet()) {
                String key = entry.getKey();
                Integer value = entry.getValue();
                context.write(new Text(key), new Text(bookName + "_" + value.toString()));
            }
        }
    }

    public static class TReducer extends Reducer<Text, Text, Text, Text> {
        private int total;

        @Override
        protected void setup(Context context) {
            total = Integer.parseInt(context.getConfiguration().get("total"));
        }

        @Override
        public void reduce(Text word, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            String w = word.toString();
            HashMap<String, Integer> tfMap = new HashMap<>();
            for (Text i : value) {
                String[] buf = i.toString().split("_");
                String bookName = buf[0];
                tfMap.put(bookName, tfMap.getOrDefault(bookName, 0) + Integer.parseInt(buf[1]));
            }
            int docnum = tfMap.size();
            double idf = Math.log10((double) total / (docnum + 1));
            for (Map.Entry<String, Integer> entry : tfMap.entrySet()) {
                String bookName = entry.getKey();
                Integer tf = entry.getValue();
                context.write(new Text(String.format("%s, %s, %s-%s", bookName, w, tf.toString(), idf)), new Text());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] stats = hdfs.listStatus(new Path(args[0]));
        int DocSum = stats.length;
        hdfs.close();
        // 全局变量传递
        conf.set("total", String.valueOf(DocSum));

        Job job = Job.getInstance(conf, "TFIDF");
        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDF.TMapper.class);
        job.setReducerClass(TFIDF.TReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
