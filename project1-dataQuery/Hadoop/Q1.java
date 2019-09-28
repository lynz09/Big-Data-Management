import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Q1 {

    public static class CustomerAgeMapper extends Mapper<Object, Text, IntWritable, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 提取value信息
            String CustomerData = value.toString();
            String[] info = CustomerData.split(",");
            int age = Integer.parseInt(info[2]);
            if (20 <= age && age <= 50) {
                context.write(null, new Text(CustomerData));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q1");
        job.setJarByClass(Q1.class);
        job.setMapperClass(CustomerAgeMapper.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}