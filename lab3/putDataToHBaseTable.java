/*** putDataToHBaseTable ***/
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;

public class putDataToHBaseTable {
    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.out.println("Arguments: [TableName] [Row] [Family] [Qualifier] [Value]");
            System.exit(1);
        }
        TableName tableName = TableName.valueOf(args[0]);
        byte[] rowKey = Bytes.toBytes(args[1]);
        byte[] family = Bytes.toBytes(args[2]);
        byte[] qualifier = Bytes.toBytes(args[3]);
        byte[] value = Bytes.toBytes(args[4]);

        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table table = connection.getTable(tableName);

        Put put = new Put(rowKey);
        put.addColumn(family, qualifier, value);

        try {
            table.put(put);

        } catch (Exception e) {
            System.out.println("The column family you're trying to put does not exist.\n");
            System.out.println("We're adding the column family --> "+family);

            Admin admin = connection.getAdmin();
            
            admin.disableTable(tableName);

            HColumnDescriptor cf = new HColumnDescriptor(args[2]);
            admin.addColumn(tableName, cf);      // adding new ColumnFamily
            
            admin.enableTable(tableName);
            
            table.put(put);

        } finally {
            table.close();
            connection.close();
        }
    }
};
