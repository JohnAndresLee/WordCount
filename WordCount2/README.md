## 问题二

### map函数的构建

由于本题需要考虑年份并输出至对应年份的文件夹，因此需要重构partition类，而在partition步骤之前的map也需要进行修改

将年份和单词组合作为key，value依然为Intwritable one，示例如下：

2008/01/01 word1 word2 ——> <"2008word1", one>, <"2008word2", one>

2016/01/01 word1 word2 ——> <"2016word1", one>, <"2016word2", one>

map函数如下所示：

```java
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String line = (caseSensitive) ?
        value.toString() :value.toString().toLowerCase();
    String year = line.substring(0, 4);
    line = line.replaceAll("[^A-Za-z ]", " ");
    StringTokenizer itr = new StringTokenizer(line);
    while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        String tmp = word.toString();
        if (!patternsToSkip.contains(tmp)){
            // 组合年份和单词
            Text ans = new Text(year+tmp);
            context.write(ans, one);
            Counter counter = context.getCounter(CountersEnum.class.getName(),
                                                 CountersEnum.INPUT_WORDS.toString());
            counter.increment(1);
        }
    }
}
```

### Partitioner函数的构建

由于字符串key的前四位是单词所在新闻的年份，因此只要取substring后进行转换就可以得到partition的结果：

```java
public static class CustomPartitioner extends Partitioner<IntWritable, Text>{
    @Override
    public int getPartition(IntWritable intWritable, Text text, int numPartitions) {
        Text text2 = text;
        String t = text2.toString().substring(0,4);
        return Integer.parseInt(t)-2008;
    }
}
```

### 根据词频排序

该部分问题二与问题一完全一致，故不再赘述。

### 自定义输出格式

问题二的该部分与问题一的区别在于：

1. **设置不同年份的输出文件名称**

2. **去除key中的年份**

MyRecorderWriter类如下所示：

```java
public static class  MyRecorderWriter extends RecordWriter<IntWritable, Text>{
        private FSDataOutputStream out;
        private String outdoor;
        private FileSystem fileSystem;
        private int num = 1;
        private Path path;
        public MyRecorderWriter(TaskAttemptContext job) throws IOException {
            outdoor = job.getConfiguration().get(FileOutputFormat.OUTDIR);
            fileSystem = FileSystem.get(job.getConfiguration());
            path = new Path(outdoor + "/problem2_out"+Integer.toString(
                    new Random().nextInt(Integer.MAX_VALUE))+".txt");
            out = fileSystem.create(path);
        }
        @Override
        public void write(IntWritable key, Text value) throws IOException, InterruptedException {
            if (num<=100) {
                out.writeBytes(String.valueOf(num));
                out.write(':');
                String tmp = value.toString();
                String tmp2 = tmp;
                tmp = tmp.replaceAll("[^A-Za-z ]", "");
                out.writeBytes(tmp);
                out.write(',');
                out.writeBytes(String.valueOf(key.get()));
                out.write('\n');
                num += 1;
                if(fileSystem.exists(path)){
                    fileSystem.rename(path, new Path(outdoor + "/problem2_out"+tmp2.substring(0,4)+".txt"));
                }
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            out.close();
        }
    }
```

### 结果展示

执行如下命令：

![截图 2022-10-23 13-36-33](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/%E6%88%AA%E5%9B%BE%202022-10-23%2013-36-33.png)

HDFS中得到的2008年的结果展示为：

![截图 2022-10-23 13-37-48](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/%E6%88%AA%E5%9B%BE%202022-10-23%2013-37-48.png)