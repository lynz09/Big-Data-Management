import java.awt.peer.SystemTrayPeer;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

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


public class Q4 {
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
                        CustomerInfoMap.put(split[0], split[4]);
                    }
                }
            }
        }


        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] Trans = value.toString().split(",");
            String valueSlitted = CustomerInfoMap.get(Trans[1]);
            //System.out.println(Arrays.toString(valueSlitted));
            context.write(new Text(valueSlitted), new Text(Trans[1] +"," +Trans[2])); // Key: countrycode,value: ID +
            // Transtotal
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            float minTransTotal = 100000.0f;
            float maxTransTotal = 0.0f;
            //int count = 0;
            Set<Integer> hashSet = new HashSet<Integer>();
            for (Text t : values) {
                String[] l = t.toString().split(",");
                int CustID = Integer.parseInt(l[0]);
                float Transtotal = Float.parseFloat(l[1]);
                hashSet.add(CustID);
                //System.out.println(hashSet);
                    //count ++;

                if (Transtotal < minTransTotal) minTransTotal = Transtotal;
                if (Transtotal > maxTransTotal) maxTransTotal = Transtotal;

            }
            int count = hashSet.size();
            context.write(key, new Text(count + "," + minTransTotal + "," + maxTransTotal));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Q4");
        //job.setNumReduceTasks(0);
        job.setJarByClass(Q4.class);
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




