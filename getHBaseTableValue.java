/*** getHBaseTableValue ***/
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class getHBaseTableValue {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Arguments: [TableName] [Row] [Family] [Qualifier]");
            System.exit(1);
        }
        TableName tableName = TableName.valueOf(args[0]);
        byte[] rowKey = Bytes.toBytes(args[1]);
        byte[] family = Bytes.toBytes(args[2]);
        byte[] qualifier = Bytes.toBytes(args[3]);
        
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Table table = connection.getTable(tableName);

        Get get = new Get(rowKey);        

        try{
            Result rowResult = table.get(get);            
            byte[] value = rowResult.getValue(family, qualifier);
            System.out.println("Value: " + Bytes.toString(value));
        }finally{
            table.close();
            connection.close();
        }
    }
};
