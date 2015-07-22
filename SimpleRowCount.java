import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * The.Definitive.Guide.4th  示例代码
 * 查询输入表中行数,通过查询得到每个主键与对应的第一键值 tom  course:english
 *
 * 通过这种方式查询输出结果还需要处理
 *
 * Created by fly on 15-7-10.
 */
public class SimpleRowCount extends Configured implements Tool {


    static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
        public static enum Counters { ROWS }
   //     private NcdcRecordParser parser = new NcdcRecordParser();
        /**
         *
         * @param row 为主键，不能通过toString直接转换
         * @param value 取出后得到jack/course:english/1436449989754/Put/vlen=2/seqid=0，不能单独取出
         * @param context
         */
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws UnsupportedEncodingException {
            context.getCounter(Counters.ROWS).increment(1);
            System.out.println(context.getCounter(Counters.ROWS).getValue());
            String s = new String(row.copyBytes(), "GB2312");
            System.out.println(s);

            List<Cell> valuelist = value.getColumnCells("course".getBytes(), "english".getBytes());
            //得到结果为  valuelist[0] = "jack/course:english/1436449989754/Put/vlen=2/seqid=0"

            for(Cell cell : valuelist) {
                System.out.println(cell.toString());
            }
           // System.out.println(value.getColumnCells("course".getBytes(),"english".getBytes())+"\n");

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: SimpleRowCounter <tablename>");
            return -1;
        }
        String tableName = args[0];
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        Job job = new Job(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());      //Set the Jar by finding where a given class came from.
        TableMapReduceUtil.initTableMapperJob(tableName, scan,
                RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);

        job.setNumReduceTasks(0);   //不需要reduce过程
        job.setOutputFormatClass(NullOutputFormat.class);   //不需要输出
        return job.waitForCompletion(true) ? 0 : 1;
    }

    //main函数输入一个参数
    public static void main(String[] args1) throws Exception {
        String[] args = new String[1];
        args[0] = "score";
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new SimpleRowCount(), args);
        System.exit(exitCode);
    }
}