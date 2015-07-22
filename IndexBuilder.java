import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.client.Result;


import java.io.IOException;
import java.util.HashMap;

/**
 * Created by fly on 15-7-10.
 */
public class IndexBuilder {

    public static final byte[] INDEX_COLUMN = Bytes.toBytes("INDEX");
    public static final byte[] INDEX_QUALIFIER = Bytes.toBytes("ROW");

    public static class Map extends Mapper<ImmutableBytesWritable, Result,ImmutableBytesWritable,Put>{
        private byte[] family;

        private HashMap<byte[],ImmutableBytesWritable> indexes;

        protected void map(ImmutableBytesWritable rowKey, Result result, Context context)
        throws IOException,InterruptedException{
            for(java.util.Map.Entry<byte[],ImmutableBytesWritable> index:indexes.entrySet()){
                byte[] qualifier = index.getKey();
                ImmutableBytesWritable tableName = index.getValue();
                byte[] value = result.getValue(family,qualifier);

                if (value != null) {
                    Put put = new Put(value);
                    put.addColumn(INDEX_COLUMN,INDEX_QUALIFIER,rowKey.get());

                    context.write(tableName,put);


                }


            }
        }

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration configuration = context.getConfiguration();

            String tableName = configuration.get("index.tablename");
            String[] fields = configuration.getStrings("index.fields");

            String familyName = configuration.get("index.familyname");
            family = Bytes.toBytes(familyName);

            indexes = new HashMap<byte[], ImmutableBytesWritable>();
            for(String field:fields){
                indexes.put(Bytes.toBytes(field),new ImmutableBytesWritable(Bytes.toBytes(tableName + "-" + field)));
            }
        }

        public static Job configureJob(Configuration conf, String[] args)
            throws IOException{
            String tableName = args[0];
            String columnFamily = args[1];
            System.out.println("****" + tableName);

           // conf.set(TableInputFormat.SCAN)
            Job job = new Job(conf,tableName);
            return job;
        }
    }




}
