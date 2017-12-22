
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author vahid
 */
public class BusyMonth {

    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String terminal = value.toString().split(",")[2];
            String date = value.toString().split(",")[1].split(" ")[0];
            String month_year = date.substring(0, 2) + date.substring(5, 10);
            long passenger_count = Long.valueOf(value.toString().split(",")[5]);
            if (terminal.startsWith("Terminal") || terminal.startsWith("Tom Bradley")) {
                context.write(new Text(month_year), new LongWritable(passenger_count));
            }
        }

    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            if (sum > 5000000) {
                context.write(key, new LongWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf0 = new Configuration();
        Job job0 = new Job(conf0, "Busy Month");
        job0.setJarByClass(BusyMonth.class);
        job0.setMapperClass(BusyMonth.Map.class);
        job0.setReducerClass(BusyMonth.Reduce.class);

        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job0, new Path(args[0]));
        FileOutputFormat.setOutputPath(job0, new Path(args[1]));

        job0.waitForCompletion(true);
    }

}
