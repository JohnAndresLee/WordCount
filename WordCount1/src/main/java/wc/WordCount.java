package wc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import com.google.common.base.Objects;
//import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.util.StringUtil;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        static enum CountersEnum {INPUT_WORDS};
        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() :value.toString().toLowerCase();
            line = line.replaceAll("[^A-Za-z ]", " ");
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String tmp = word.toString();
                if (!patternsToSkip.contains(tmp)){
                    context.write(word, one);
                    Counter counter = context.getCounter(CountersEnum.class.getName(),
                            CountersEnum.INPUT_WORDS.toString());
                    counter.increment(1);
                }
            }
        }

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName){
            try{
                BufferedReader fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine())!=null){
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file"+
                        StringUtils.stringifyException(ioe));
            }
        }
    }


    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class  MyRecorderWriter extends RecordWriter<IntWritable, Text>{
        private FSDataOutputStream out;
        private int num = 1;
        public MyRecorderWriter(TaskAttemptContext job) throws IOException {
            String outdoor = job.getConfiguration().get(FileOutputFormat.OUTDIR);
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            out = fileSystem.create(new Path(outdoor + "/problem1_out.txt"));
        }
        @Override
        public void write(IntWritable key, Text value) throws IOException, InterruptedException {
            if (num<=100) {
                out.writeBytes(String.valueOf(num));
                out.write(':');
                out.writeBytes(value.toString());
                out.write(',');
                out.writeBytes(String.valueOf(key.get()));
                out.write('\n');
                num += 1;
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            out.close();
        }
    }

    public  static class MyOutputFormat extends FileOutputFormat<IntWritable, Text>{
        @Override
        public RecordWriter<IntWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            MyRecorderWriter myRecorderWriter = new MyRecorderWriter(job);
            return myRecorderWriter;
        }
    }

    //对value降序排序
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        // the length of remainingArgs is 2 or 4
        if (!(remainingArgs.length!=2 || remainingArgs.length!=4)){
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }
        FileSystem fs = FileSystem.get(conf);
        //定义一个临时目录
        Path tempDir = new Path("wordcount-temp-"+Integer.toString(
                new Random().nextInt(Integer.MAX_VALUE)));

        Job job = Job.getInstance(conf, "wordcount 1.0");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
//        job.setOutputFormatClass(FlowOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, tempDir);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if(job.waitForCompletion(true))
        {
            Job sortJob = new Job(conf, "sort");
            sortJob.setJarByClass(WordCount.class);

            FileInputFormat.addInputPath(sortJob, tempDir);
            sortJob.setInputFormatClass(SequenceFileInputFormat.class);

            /*InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换*/
            sortJob.setMapperClass(InverseMapper.class);
            /*将 Reducer 的个数限定为1, 最终输出的结果文件就是一个。*/
            sortJob.setNumReduceTasks(1);

            FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs.get(1)));

            sortJob.setOutputFormatClass(MyOutputFormat.class);

            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);
            /*Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。
             * 因此我们实现了一个 IntWritableDecreasingComparator 类,
             * 并指定使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行排序*/
            sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
            if(sortJob.waitForCompletion(true))//删除中间文件
                fs.delete(tempDir);
            System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
        }
    }
}
