import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 权威指南第四版示例代码
 * 过时类的使用已经更新
 *
 * Created by fly on 15-7-10.
 */





public class HbaseBasicOper_new {
    public static void main(String[] args) throws IOException {

        //创建管理用户
        Configuration conf = HBaseConfiguration.create();   //会调用
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        try {

            //创建新表
            TableName tableName = TableName.valueOf("test1");
            HTableDescriptor htd = new HTableDescriptor(tableName);
            HColumnDescriptor hcd = new HColumnDescriptor("data");
            htd.addFamily(hcd);
            admin.createTable(htd);
            HTableDescriptor[] tables = admin.listTables();
            if (tables.length != 1 &&       //不是！=0 ？？  当数据库中没有表时返回的tables得到的是1
                    Bytes.equals(tableName.getName(), tables[0].getTableName().getName())) {
                throw new IOException("Failed create of table");
            }


// Run some operations -- three puts, a get, and a scan -- against the table.
            HTable table = (HTable)connection.getTable(tableName);

                  //  put操作  添加数据
            try {
                for (int i = 1; i <= 3; i++) {
                    byte[] row = Bytes.toBytes("row" + i);
                    Put put = new Put(row);
                    byte[] columnFamily = Bytes.toBytes("data");
                    byte[] qualifier = Bytes.toBytes(String.valueOf(i));
                    byte[] value = Bytes.toBytes("value" + i);
                    put.addColumn(columnFamily, qualifier, value);
                    table.put(put);
                }

                //get操作，从对应table中获取相应键值的一行
                //get操作得到的是一行，通过scan先得到所有的列，然后从列中取得行
                Get get = new Get(Bytes.toBytes("row1"));
                Result result = table.get(get);
                System.out.println("Get: " + result);


                //scan 操作，从对应的table中获取相应的一列
                //可以添加限制条件
                Scan scan = new Scan();//使用完必须关闭
                ResultScanner scanner = table.getScanner(scan);
                try {
                    for (Result scannerResult : scanner) {
                        System.out.println("Scan: " + scannerResult);
                    }
                } finally {
                    scanner.close();
                }

                //删除表格
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            } finally {
                table.close();
            }
        } finally {
            admin.close();
        }
    }
}