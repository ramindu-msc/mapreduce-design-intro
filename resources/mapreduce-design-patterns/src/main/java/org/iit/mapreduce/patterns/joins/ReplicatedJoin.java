package org.iit.mapreduce.patterns.joins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReplicatedJoin {

    public static class CountryMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, String> countryMap = new HashMap<>();



        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Assume input format for order data: orderId,customerId,countryCode,amount
            String[] orderFields = value.toString().split(",");
            if (orderFields.length >= 3) {
                String countryCode = orderFields[2].trim();
                String countryName = countryMap.get(countryCode);
                if (countryName != null) {
                    // Emit the joined record
                    context.write(new Text(countryCode), new Text(countryName + "," + value.toString()));
                }
            }
        }
    }

    public static class CountryOrderReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                // Emit the joined data
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ReplicatedJoin <country data> <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("country.data.path", "<hdfs-path-to-country-data>");

        Job job = Job.getInstance(conf, "Replicated Join");
        job.setJarByClass(ReplicatedJoin.class);
        job.setMapperClass(CountryMapper.class);
        job.setReducerClass(CountryOrderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1); // Use one reducer to consolidate output

        FileInputFormat.addInputPath(job, new Path(args[1])); // Input path for order data
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Output path for joined data

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

