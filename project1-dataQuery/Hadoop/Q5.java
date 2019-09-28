import java.awt.peer.SystemTrayPeer;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Q5 {
    public static class MapSideJoin extends Mapper<Object, Text, Text, Text>{
        Map<String,String> CustomerInfoMap = new HashMap<String,String>();

        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = null;
            String customer = null;
            Path[] Paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path p : Paths) {
                if (p.toString().endsWith("Customers")) {
                    br = new BufferedReader(new FileReader(p.toString()));
                    while (null != (customer = br.readLine())) {
                        String[] split = customer.split(",");
                        String AgeRange = "";
                        int Age = Integer.parseInt(split[2]);
                        if (Age >= 10 && Age < 20) AgeRange = "[10,20)";
                        if (Age >= 20 && Age < 30) AgeRange = "[20,30)";
                        if (Age >= 30 && Age < 40) AgeRange = "[30,40)";
                        if (Age >= 40 && Age < 50) AgeRange = "[40,50)";
                        if (Age >= 50 && Age < 60) AgeRange = "[50,60)";
                        if (Age >= 60 && Age <= 70) AgeRange = "[60,70]";
                        CustomerInfoMap.put(split[0], AgeRange + "," + split[3]);
                    }
                    }
                }
            }


        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] Trans = value.toString().split(",");
            String valueSlitted = CustomerInfoMap.get(Trans[1]);
            //System.out.println(Arrays.toString(valueSlitted));
            context.write(new Text(valueSlitted), new Text(Trans[2])); // Key: age range + "," + gender
            // Value: transTotal
        }
}


    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            float minTransTotal = 100000.0f;
            float maxTransTotal = 0.0f;
            float sumTransTotal = 0.0f;
            float avgTransTotal = 0.0f;
            int count = 0;

            for (Text t : values) {
                String l = t.toString();
                float ll = Float.parseFloat(l);
                if (ll < minTransTotal) minTransTotal = ll;
                if (ll > maxTransTotal) maxTransTotal = ll;
                count++;
                sumTransTotal += ll;
            }

            avgTransTotal = sumTransTotal / count;

            context.write(key, new Text(minTransTotal + "," + maxTransTotal + "," + avgTransTotal));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Q5");
        //job.setNumReduceTasks(0);
        job.setJarByClass(Q5.class);
        job.setMapperClass(MapSideJoin.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        DistributedCache.addCacheFile(new URI("/Users/zhanglin/Documents/Customers"), job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);




    }
    }



