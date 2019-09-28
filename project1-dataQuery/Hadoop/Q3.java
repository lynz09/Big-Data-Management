import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Q3 {
    static class CustomerMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String CusInfo = value.toString();
            String[] CusData = CusInfo.split(",");
            String CusName = CusData[1];
            float Salary = Float.parseFloat(CusData[5]);
            String CusID = CusData[0];
            context.write(new Text(CusID), new Text("cust" + "," + CusName + ","+ Salary));
        }
    }

    static class TransMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String TransInfo = value.toString();
            String[] TransData = TransInfo.split(",");
            String CusID = TransData[1];
            float TransTotal = Float.parseFloat(TransData[2]);
            int TransNumI = Integer.parseInt((TransData[3]));
            context.write(new Text(CusID), new Text("trans"+ "," + TransTotal + "," + TransNumI ));
        }
    }

    public static class FinalReduer extends Reducer<Text, Text, Text, Text>{
        public  void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
//            Iterator<Text> which = values.iterator();
            String CusName = "";
            float Salary = 0.000f;
            int NumTrans = 0;
            float TotalSum = 0;
            int MinItem = -1;

            for (Text set : values){
                String[] which = set.toString().split(",");
                if(which[0].equals("cust")){
                    CusName = which[1];
                    Salary =  Float.parseFloat(which[2]);
                }
                else if (which[0].equals("trans")){
                    int Item = Integer.parseInt(which[2]);
                    NumTrans++;
                    TotalSum += Float.parseFloat(which[1]);
                    if (MinItem == -1 || MinItem > Item){
                        MinItem = Item;
                    }
                }
            }
            Text FH = new Text(key + "," + CusName + "," +Salary + "," + NumTrans + "," +TotalSum + "," +MinItem);
            context.write(null, FH);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q3");
        job.setJarByClass(Q3.class);
        job.setReducerClass(FinalReduer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CustomerMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

