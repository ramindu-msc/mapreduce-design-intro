package org.iit.mapreduce.patterns.joins;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CompositeJoin extends Configured implements Tool {

    public static class UserLogsMapper extends Mapper<Object, Text, Text, Text> {
        private Text userId = new Text();
        private Text logInfo = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 1) {
                userId.set(fields[0]);  // Assuming user_id is the first column
                logInfo.set("LOGS:" + value.toString());
                context.write(userId, logInfo);
            }
        }
    }

    public static class UserProfilesMapper extends Mapper<Object, Text, Text, Text> {
        private Text userId = new Text();
        private Text profileInfo = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 1) {
                userId.set(fields[0]);  // Assuming user_id is the first column
                profileInfo.set("PROFILE:" + value.toString());
                context.write(userId, profileInfo);
            }
        }
    }

    public static class JoinMapper extends Mapper<Text, Text, Text, Text> {
        private List<String> userLogs = new ArrayList<>();
        private List<String> userProfiles = new ArrayList<>();

        @Override
        protected void setup(Context context) {
            userLogs.clear();
            userProfiles.clear();
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("LOGS:")) {
                userLogs.add(line.substring(5));
            } else if (line.startsWith("PROFILE:")) {
                userProfiles.add(line.substring(8));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String log : userLogs) {
                for (String profile : userProfiles) {
                    context.write(new Text("JoinedRecord"), new Text(log + "," + profile));
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Composite Join");
        job.setJarByClass(CompositeJoin.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserLogsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserProfilesMapper.class);

        job.setMapperClass(JoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);  // Map-only job

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CompositeJoin(), args);
        System.exit(exitCode);
    }
}
