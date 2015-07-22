import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;


/**
 * 使用测试数据建表并添加查询计算功能
 *
 * Created by fly on 15-7-10.
 */
public class testDataProcess {
    static int count = 0;

    public static void createNewTable(Admin admin) throws IOException {
        //创建新表
        TableName tableName = TableName.valueOf("testDataProcess");
        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(new HColumnDescriptor("coordinate"));
        htd.addFamily(new HColumnDescriptor("attribute"));

        admin.createTable(htd);
        HTableDescriptor[] tables = admin.listTables();
        if (tables.length != 0 &&
                Bytes.equals(tableName.getName(), tables[0].getTableName().getName())) {
            throw new IOException("Failed create of table");
        }
    }

    public static void putFileData(Table table) throws IOException {
        Scanner in = new Scanner(new File("/home/fly/桌面/hadoop初步实现/testData.csv"));

        String str;
        String[] strs;
        while (in.hasNextLine()) {
            str = in.nextLine();
            strs = str.split(",");

            Put put = new Put(Bytes.toBytes(strs[0]));
            put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("x"), Bytes.toBytes(strs[1]) );
            put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("y"), Bytes.toBytes(strs[2]) );
            put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("z"), Bytes.toBytes(strs[3]) );
            put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("atr"), Bytes.toBytes(strs[4]) );
            put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("atrr"), Bytes.toBytes(strs[5]) );
            table.put(put);

        }
    }

    public static void getData(Table table,String key){
        Get get = new Get(Bytes.toBytes(key));
  //      get.addColumn(Bytes.toBytes("coordinate"),Bytes.toBytes("x"));   // @nullable定义的字段可能为空
        Result result = null;
    //    SimpleDateFormat time=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    //    String timeString = time.format(new java.util.Date());
       // System.out.println(timeString);
        try {
            result = table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }

     //   System.out.println(time.format(new java.util.Date()));
        System.out.println("Get: " + Bytes.toString(result.getValue(Bytes.toBytes("coordinate"),Bytes.toBytes("x"))));
        System.out.println("Get: " + Bytes.toString(result.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("y"))));
        System.out.println("Get: " + Bytes.toString(result.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("z"))));


        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("z"));
//        scan.setStartRow(Bytes.toBytes("F11：#"));    //这种方式会得到11  120-129 130-139等数据
//        scan.setStopRow(Bytes.toBytes("F15：："));

        //使用filter取z坐标大于38的数据
        scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("coordinate"),Bytes.toBytes("z"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("38") ));

        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);//使用完必须关闭
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            for (Result scannerResult : scanner) {
                count++;
                System.out.println(count);
                System.out.println("Scan: " + Bytes.toString(scannerResult.getValue(Bytes.toBytes("coordinate"),Bytes.toBytes("z"))) + "\n");
            }
        } finally {
            scanner.close();
        }

    }

    public static void main(String[] args) throws IOException {

        //创建管理用户
        Configuration conf = HBaseConfiguration.create();   //会调用
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        HTable table;
        TableName tableName = TableName.valueOf("testDataProcess");
        table = (HTable)connection.getTable(tableName);

        try {
      //      createNewTable(admin);
     //       putFileData(table);
            getData(table,"F5");
        } finally {
            table.close();
            admin.close();
        }
    }

}
