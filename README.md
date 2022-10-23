# WordCount

在HDFS上加载Reddit WorldNews Channel热点新闻标题数据集（RedditNews.csv），该数据集收集了2008-06-08至2016-07-01每日的TOP25热点财经新闻标题。编写MapReduce程序进行词频统计，并按照单词出现次数从大到小排列，输出

（1）数据集出现的前100个高频单词；

（2）每年热点新闻中出现的前100个高频单词。要求忽略大小写，忽略标点符号，忽略停词（stop-word-list.txt）。

输出格式为"<排名>：<单词>，<次数>“，输出可以根据年份不同分别写入不同的文件，也可以合并成一个文件。

## 问题一



### map函数的构建


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

### 自定义输出格式

## 问题二