import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;

/** 通过MapReduce框架读取HBase中的内容，在Map函数中输出结果
 * TODO 读取HDFS中的二进制文件，上传到HBase中
 *
 * Created by fly on 15-7-11.
 */
public class HBaseMapReduce_2TableMapper extends Configured implements Tool {


    static class HBaseMapper extends TableMapper<Text, DoubleWritable>
    {

        @Override
        public void map(ImmutableBytesWritable rowKey, Result columns,
                        Context context) throws IOException, InterruptedException {
            System.out.println(columns.getColumnCells(Bytes.toBytes("coordinate"), Bytes.toBytes("x")));
     //       System.out.println(Bytes.toString(rowKey.copyBytes()));
            System.out.println(rowKey.copyBytes());

        }
    }

    @Override
    public	int	run(String[]	args)	throws	Exception	{

        Job	job	=	new	Job(getConf(),	"Max	temperature");
        job.setJarByClass(getClass());


        //设置HBase中的读取参数
        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);
        //scan.setMaxResultSize(20);
        scan.setStartRow(Bytes.toBytes("F5"));
        scan.setStopRow(Bytes.toBytes("F8"));
        scan.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("x"));

        TableMapReduceUtil.initTableMapperJob(
                "testDataProcess", // input table
                scan, // Scan instance to control CF and attribute selection
                HBaseMapper.class, // mapper class
                Text.class, // mapper output key
                IntWritable.class, // mapper output value
                job);


        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output13" +
                ""));
        job.setMapperClass(HBaseMapper.class);



        return	job.waitForCompletion(true)	?	0	:	1;
    }

    public	static	void	main(String[]	args)	throws	Exception	{
        int	exitCode = ToolRunner.run(new HBaseMapReduce_2TableMapper(),	args);

        System.exit(exitCode);
    }




}


