/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eastcircle.terasort;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream; // eastcirclek
import java.io.ObjectOutputStream; // eastcirclek
import java.io.PrintStream;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * eastcirclek : 
 * I extract this class from org.apache.hadoop.examples.terasort.TeraSort 
 * and make little modification to use it for TeraSort on Spark and Flink.
 * - We no longer implement org.apache.hadoop.mapreduce.Partitioner
 *   as we are going to use this class for custom partitioners in 
 *   Spark (org.apache.spark.Partitioner) and 
 *   Flink (org.apache.flink.api.common.functions.Partitioner).
 * - We implement java.io.Serializable because Spark and Flink pass
 *   partitioner objects to tasks by serializing/deserializing the objects.
 * - We remove setConf() from the origial TotalOrderPartitioner
 *   as it assumes that TeraInputFormat.PARTITION_FILENAME is 
 *	 localized to PWD on which a MapReduce task is running.
 **/

/**
 * A partitioner that splits text keys into roughly equal partitions
 * in a global sorted order.
 */
public class TotalOrderPartitioner implements Serializable{

    private Configuration conf;
    private TrieNode trie;

    private Path partFile; // eastcirclek
	
    /**
     * A generic trie node
     */
    static abstract class TrieNode {
        private int level;
        TrieNode(int level) {
            this.level = level;
        }
        abstract int findPartition(Text key);
        abstract void print(PrintStream strm) throws IOException;
        int getLevel() {
            return level;
        }
    }

    /**
     * An inner trie node that contains 256 children based on the next
     * character.
     */
    static class InnerTrieNode extends TrieNode {
        private TrieNode[] child = new TrieNode[256];
      
        InnerTrieNode(int level) {
            super(level);
        }
        int findPartition(Text key) {
            int level = getLevel();
            if (key.getLength() <= level) {
                return child[0].findPartition(key);
            }
            return child[key.getBytes()[level] & 0xff].findPartition(key);
        }
        void setChild(int idx, TrieNode child) {
            this.child[idx] = child;
        }
        void print(PrintStream strm) throws IOException {
            for(int ch=0; ch < 256; ++ch) {
                for(int i = 0; i < 2*getLevel(); ++i) {
                    strm.print(' ');
                }
                strm.print(ch);
                strm.println(" ->");
                if (child[ch] != null) {
                    child[ch].print(strm);
                }
            }
        }
    }

    /**
     * A leaf trie node that does string compares to figure out where the given
     * key belongs between lower..upper.
     */
    static class LeafTrieNode extends TrieNode {
        int lower;
        int upper;
        Text[] splitPoints;
        LeafTrieNode(int level, Text[] splitPoints, int lower, int upper) {
            super(level);
            this.splitPoints = splitPoints;
            this.lower = lower;
            this.upper = upper;
        }
        int findPartition(Text key) {
            for(int i=lower; i<upper; ++i) {
                if (splitPoints[i].compareTo(key) >= 0) {
                    return i;
                }
            }
            return upper;
        }
        void print(PrintStream strm) throws IOException {
            for(int i = 0; i < 2*getLevel(); ++i) {
                strm.print(' ');
            }
            strm.print(lower);
            strm.print(", ");
            strm.println(upper);
        }
    }


    /**
     * Read the cut points from the given sequence file.
     * @param fs the file system
     * @param p the path to read
     * @param job the job config
     * @return the strings to split the partitions on
     * @throws IOException
     */
    private static Text[] readPartitions(FileSystem fs, Path p, Configuration conf) throws IOException {
        int reduces = conf.getInt(MRJobConfig.NUM_REDUCES, 1);
        Text[] result = new Text[reduces - 1];
        DataInputStream reader = fs.open(p);
        for(int i=0; i < reduces - 1; ++i) {
            result[i] = new Text();
            result[i].readFields(reader);
        }
        reader.close();
        return result;
    }

    /**
     * Given a sorted set of cut points, build a trie that will find the correct
     * partition quickly.
     * @param splits the list of cut points
     * @param lower the lower bound of partitions 0..numPartitions-1
     * @param upper the upper bound of partitions 0..numPartitions-1
     * @param prefix the prefix that we have already checked against
     * @param maxDepth the maximum depth we will build a trie for
     * @return the trie node that will divide the splits correctly
     */
    private static TrieNode buildTrie(Text[] splits, int lower, int upper, Text prefix, int maxDepth) {
        int depth = prefix.getLength();
        if (depth >= maxDepth || lower == upper) {
            return new LeafTrieNode(depth, splits, lower, upper);
        }
        InnerTrieNode result = new InnerTrieNode(depth);
        Text trial = new Text(prefix);
        // append an extra byte on to the prefix
        trial.append(new byte[1], 0, 1);
        int currentBound = lower;
        for(int ch = 0; ch < 255; ++ch) {
            trial.getBytes()[depth] = (byte) (ch + 1);
            lower = currentBound;
            while (currentBound < upper) {
                if (splits[currentBound].compareTo(trial) >= 0) {
                    break;
                }
                currentBound += 1;
            }
            trial.getBytes()[depth] = (byte) ch;
            result.child[ch] = buildTrie(splits, lower, currentBound, trial, 
                                         maxDepth);
        }
        // pick up the rest
        trial.getBytes()[depth] = (byte) 255;
        result.child[255] = buildTrie(splits, currentBound, upper, trial,
                                      maxDepth);
        return result;
    }

    // eastcirclek
    private static TrieNode buildTrieFromHDFS(Configuration conf, Path hdfsPath) throws IOException{
        FileSystem fs = hdfsPath.getFileSystem(conf);
        Text[] splitPoints = readPartitions(fs, hdfsPath, conf);
        return buildTrie(splitPoints, 0, splitPoints.length, new Text(), 2);
    }

    // eastcirclek
    public TotalOrderPartitioner(Configuration conf,
                                 Path partFile) throws IOException{
        this.conf = conf;
        this.partFile = partFile;
        this.trie = buildTrieFromHDFS(conf, partFile);
    }

    // eastcirclek
    public int getPartition(Text key){
        return trie.findPartition(key);
    }

    // eastcirclek for serialization
    private void writeObject(ObjectOutputStream out) throws IOException{
        out.writeUTF(conf.get("fs.defaultFS"));
        out.writeInt(conf.getInt(MRJobConfig.NUM_REDUCES, 2));
        /**
         * Instead of serializing the trie,
         *  we serialize the filename containing sampling points
         *  so that we can rebuild the trie in each task.
         */
        out.writeUTF(this.partFile.toString());
    }

    // eastcirclek for deserialization
    private void readObject(ObjectInputStream in) throws IOException{
        this.conf = new Configuration();
        conf.set("fs.defaultFS", (String)in.readUTF());
        conf.setInt(MRJobConfig.NUM_REDUCES, (int)in.readInt());
        this.partFile = new Path((String)in.readUTF());
        this.trie = buildTrieFromHDFS(conf, partFile);
    }
}
