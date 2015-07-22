import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 实现通过Map函数对文件进行读取,Mapper类不直接调用HBase
 *
 * Created by fly on 15-7-11.
 */
public class HBaseMapReduce_1put extends Configured implements Tool {

    Connection connection;
    static HTable table;



    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        //输入的value为文本文件的一行，key为文本文件对应行相对文本文件首地址的偏移量
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] strs = value.toString().split(",");

            Put put = new Put(Bytes.toBytes(strs[0]));
            put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("x"), Bytes.toBytes(strs[1]));
            put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("y"), Bytes.toBytes(strs[2]));
            put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("z"), Bytes.toBytes(strs[3]));
            put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("atr"), Bytes.toBytes(strs[4]));
            put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("atrr"), Bytes.toBytes(strs[5]));
            table.put(put);


        }
    }



    @Override
    public	int	run(String[]	args)	throws	Exception	{

        Job	job	=	new	Job(getConf(),	"Max	temperature");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/testDataProcess"));

        //输出路径必须设置，会产生结果文件，如果没有输出结果文件为空
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output2"));

        job.setMapperClass(TokenizerMapper.class);

        //获取HBase中的对应table
        Configuration conf = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("testDataProcess");
        table = (HTable)connection.getTable(tableName);
        return	job.waitForCompletion(true)	?	0	:	1;
    }

    public	static	void	main(String[]	args)	throws	Exception	{
        int	exitCode = ToolRunner.run(new HBaseMapReduce_1put(),	args);
        table.close();
        System.exit(exitCode);
    }




}
