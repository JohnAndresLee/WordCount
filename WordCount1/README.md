## 问题一

### map函数的构建

与wordcount2.0不同的是，问题一中不仅需要忽略非字母符号，还需要忽略部分单词，因此如果仍按照wordcount2.0的思路，将可能出现以下的情况：单词bad在map过后变成了bd（由于字母a在stop-word-list.txt）。因此本次实验中我分两个阶段分别处理非字母符号和停词：

1. 处理非字母符号

运用**正则表达式**，过滤掉所有的非字母与普通空格的字符

```java
line = line.replaceAll("[^A-Za-z ]", " ");
```

2. 处理停词

在写入<key,value>之前判断该单词是否在stop-word-list.txt，若在停词表中则跳过

```java
if (!patternsToSkip.contains(tmp)){
            context.write(word, one);
            Counter counter = context.getCounter(CountersEnum.class.getName(),
                    CountersEnum.INPUT_WORDS.toString());
            counter.increment(1);
        }
```

**map函数的整体代码如下所示**


```java
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String line = (caseSensitive) ?
            value.toString() :value.toString().toLowerCase();
    // 去除标题中的非字母字符
    line = line.replaceAll("[^A-Za-z ]", " ");
    StringTokenizer itr = new StringTokenizer(line);
    while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        String tmp = word.toString();
        // 跳过stop-word-list.txt中的单词
        if (!patternsToSkip.contains(tmp)){
            context.write(word, one);
            Counter counter = context.getCounter(CountersEnum.class.getName(),
                    CountersEnum.INPUT_WORDS.toString());
            counter.increment(1);
        }
    }
}
```

### 根据词频排序

由于MapReduce是根据key进行排序并输出的，因此想对词频（value）进行排序，需要对setSortComparatorClass进行重构。又由于Hadoop 默认对 IntWritable 按升序排序，而实验需要的是按降序排列，因此我实现了一个 IntWritableDecreasingComparator 类，并指定使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行排序：

```java
//对value降序排序
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
```

整个排序过程的逻辑是：先按照原始的key排序将结果输出到中间文件，再将中间文件作为输入，完成最后结果的输出，main函数里的定义：

```java
if(job.waitForCompletion(true)){
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
```

### 自定义输出格式

由于题目要求输出格式为"<排名>：<单词>，<次数>“，因此对FileOutputFormat与RecordWriter进行重构

**！！！一定要注意在class前面加上static，不然可能会报*NoSuchMethod <init>*的错误**

```java
public  static class MyOutputFormat extends FileOutputFormat<IntWritable, Text>{
    @Override
    public RecordWriter<IntWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        MyRecorderWriter myRecorderWriter = new MyRecorderWriter(job);
        return myRecorderWriter;
    }
}
```

自定义的MyRecorderWriter主要控制文件的输出路径与输出格式，需要构建write()与close()两个函数

```java
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
```

### 结果展示

执行如下的命令：

![截图 2022-10-23 13-36-57](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/%E6%88%AA%E5%9B%BE%202022-10-23%2013-36-57.png)

HDFS中得到的结果展示为：

![截图 2022-10-23 13-37-58](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/%E6%88%AA%E5%9B%BE%202022-10-23%2013-37-58.png)