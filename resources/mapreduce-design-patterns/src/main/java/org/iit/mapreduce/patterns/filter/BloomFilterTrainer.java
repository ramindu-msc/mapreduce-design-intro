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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import static java.lang.Math.*;

public class BloomFilterTrainer {
    public static void main(String[] args) throws Exception {
        Path inputFile = new Path(args[0]);
        // http://hur.st/bloomfilter
        int n = Integer.parseInt(args[1]); // number of items in the filter
        float p = Float.parseFloat(args[2]); // false positive rate
        Path outputFile = new Path(args[3]);

        int m = calculateM(n, p); // # of bits in the filter
        int k = calculateK(n, m); // # of hash functions
        System.out.println(m);
        BloomFilter filter = new BloomFilter(m, k, Hash.MURMUR_HASH);

        System.out
                .println(String
                        .format("Training Bloom filter of size %d with %d hash functions, %d approx. no. records, and %f false pos. rate",
                                m, k, n, p));

        String line = null;
        int numElements = 0;
        FileSystem fs = FileSystem.get(new Configuration());

        for (FileStatus status : fs.listStatus(inputFile)) {
            InputStream stream;
            if (status.getPath().toString().toLowerCase().endsWith("gz")) {
                stream = new GZIPInputStream(fs.open(status.getPath()));
            } else {
                stream = fs.open(status.getPath());
            }

            BufferedReader rdr = new BufferedReader(new InputStreamReader(
                    stream));
            System.out.println("Reading " + status.getPath());
            while ((line = rdr.readLine()) != null) {
                filter.add(new Key(line.getBytes()));
                ++numElements;
            }

            rdr.close();
        }

        System.out.println(String.format(
                "Trained Bloom filter with %d entries", numElements));
        System.out.println("Serializing Bloom filter to HDFS at " + outputFile);

        FSDataOutputStream stream = fs.create(outputFile);
        filter.write(stream);
        stream.flush();
        stream.close();

        System.exit(0);
    }

    private static int calculateM(int n, float p) {
        return (int) ceil((n * log(p)) / log(1.0f / (pow(2.0f, log(2.0f)))));
    }

    private static int calculateK(int n, int m) {
        return (int) round(log(2.0f) * m / n);
    }
}
