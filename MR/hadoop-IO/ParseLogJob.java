package com.bigdata.etl.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.etl.mr.LogBeanWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.com.bigdata.etl.job.com.bigdata.etl.mr.LogFieldWritable;
import java.com.bigdata.etl.job.com.bigdata.etl.mr.LogGenericWritable;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

public class ParseLogJob extends Configured implements Tool {

    public static LogGenericWritable parseLog(String row) throws ParseException {
        String[] logPart = StringUtils.split(row, "\u1111");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long timeTag = dateFormat.parse(logPart[0]).getTime();

        String activeName = logPart[1];

        JSONObject bizData = JSON.parseObject(logPart[2]);
        //因为bizData的key值和下面的抽象类key值一样的，所以可以这样，要不然需要一个个取出来

        LogGenericWritable logData = new LogWritable();
        logData.put("time_tag", new LogFieldWritable(timeTag));
        logData.put("active_name", new LogFieldWritable(activeName));
        //遍历一下
        for (Map.Entry<String, Object> entry: bizData.entrySet()){
            logData.put(entry.getKey(), new LogFieldWritable(entry.getValue()));
        }

        //logData.setActiveName(activeName);
        //logData.setTimeTag(timeTag);
       // logData.setDeviceID(bizData.getString("device_id"));
        //logData.setIp(bizData.getString("ip"));
        //logData.setOrderID(bizData.getString("order_id"));
        //logData.setProductID(bizData.getString("product_id"));
       // logData.setReqUrl(bizData.getString("req_url"));
       // logData.setSessionID(bizData.getString("session_id"));
        //logData.setUserID(bizData.getString("user_id"));


        return logData;
    }
    //这里将LogGenericWritable抽象类所用到的字段名列举出来，我理解实例化
    public static class LogWritable extends LogGenericWritable{
        protected  String[] getFieldName() {
            return  new String[]{"active_name", "session_id", "time_tag", "ip", "device_id", "req_url", "user_id", "product_id", "order_id"};
        }
    }

    public static class LogMapper extends Mapper<LongWritable, Text, LongWritable, LogGenericWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                LogGenericWritable parsedLog = parseLog(value.toString());
                context.write(key, parsedLog);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LogReducer extends Reducer<LongWritable, LogGenericWritable, NullWritable, Text> {
        public void reduce(LongWritable key, Iterable<LogGenericWritable> values, Context context) throws IOException, InterruptedException {
            for (LogGenericWritable v : values ) {
                context.write(null, new Text(v.asJsonString()));
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration config = getConf();
        Job job = Job.getInstance(config);
        //通过job设置一些参数
        job.setJarByClass(ParseLogJob.class);
        job.setJobName("parselog");
        job.setMapperClass(LogMapper.class);
        //设置reduce个数为0
//        job.setNumReduceTasks(0);
        job.setReducerClass(LogReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LogWritable.class);
        job.setOutputValueClass(Text.class);


        //输入和输出数据
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }


        //运行
        if (!job.waitForCompletion(true)) {
            throw new RuntimeException(job.getJobName() + "failed!");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ParseLogJob(), args);
        System.exit(res);
    }

}
