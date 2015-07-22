import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Map;

/**
 * 向数据库中添加新表，并在新表中添加多行数据，最后删除添加的新表
 * 过时的类已经进行了更新改正
 * Created by fly on 15-7-9.
 */


public class HbaseBasicOper {


    public static void main(String[] agrs) throws IOException {
        Configuration conf = HBaseConfiguration.create();

     //   HBaseAdmin admin1 = new HBaseAdmin(conf); 类已过时，不推荐使用


        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

     //   connection.getTable("")

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("tab6".getBytes()));
        tableDescriptor.addFamily(new HColumnDescriptor("fam1"));//添加一列
        tableDescriptor.addFamily(new HColumnDescriptor("fam2"));//添加一列
        admin.createTable(tableDescriptor);

        //instances of this class SHOULD NOT be constructed directly
        //HTable 不能直接new出对象
        HTable table = (HTable) connection.getTable(TableName.valueOf("tab6"));
        //向table中添加一行


        Put put = new Put("row1".getBytes());   //row1为行键
        put.addColumn("fam1".getBytes(),"col1".getBytes(),"val1".getBytes());
        put.addColumn("fam1".getBytes(),"col1".getBytes(), "val2".getBytes());   //覆盖上次上传的结果
        put.addColumn("fam1".getBytes(),"col2".getBytes(),"val1".getBytes());   //每个family列下可以有多个qualifier列

        table.put(put);


        //获得表中某一行（一个行键对应一行）中所有数据
        for(Result row:table.getScanner("fam1".getBytes()))
        {
            System.out.format("ROW\t%s\n", new String(row.getRow()));
            for(Map.Entry<byte[],byte[]> entry:row.getFamilyMap("fam1".getBytes()).entrySet()){
                String column = new String(entry.getKey());
                String value = new String(entry.getValue());
                System.out.format("COLUMN\tfam1:%s\t%s\n",column,value);
            }
        }


        //删除表格
        admin.disableTable(TableName.valueOf("tab6"));
        admin.deleteTable(TableName.valueOf("tab6"));
        System.out.println("end");


    }
}
