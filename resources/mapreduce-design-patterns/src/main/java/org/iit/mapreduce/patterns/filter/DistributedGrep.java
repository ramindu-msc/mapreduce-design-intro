/*
 * Licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.iit.mapreduce.patterns.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;

public class DistributedGrep {

	public static class GrepMapper extends
			Mapper<Object, Text, NullWritable, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String txt = value.toString();
			String mapRegex = context.getConfiguration().get("mapregex");

			if (txt.matches(mapRegex)) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: DistributedGrep <regex> <in> <out>");
			System.exit(2);
		}
		conf.set("mapregex", otherArgs[0]);

		Job job = new Job(conf, "Distributed Grep");
		job.setJarByClass(DistributedGrep.class);
		job.setMapperClass(GrepMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0); // Set number of reducers to zero
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
