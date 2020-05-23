/*** createHBaseTable.java ***/
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class createHBaseTable {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Arguments: [newTableName] [Family1] [Family2] ... [FamilyN]");
            System.out.println("Existing table with the same name will be deleted!");
            System.exit(1);
        }
        TableName tableName = TableName.valueOf(args[0]);
        String newColumnFamilies[] = Arrays.copyOfRange(args, 1, args.length);
        ArrayList<ColumnFamilyDescriptor> newColumnFamilyDescriptors = new ArrayList<>();

        TableDescriptor tableDescriptor = null;
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        /* Delete the table if it exists */
        if (admin.tableExists(tableName)) {
            System.out.println(tableName + " exists");
            System.out.println("disabling " + tableName + "...");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("deleting " + tableName + "...");
        }

        /* Create column families */
        for (String newColumnFamily : newColumnFamilies) {
            newColumnFamilyDescriptors
                    .add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(newColumnFamily)).build());
        }
        tableDescriptorBuilder.setColumnFamilies(newColumnFamilyDescriptors);

        System.out.println("creating " + tableName + "...");
        tableDescriptor = tableDescriptorBuilder.build();
        try {
            admin.createTable(tableDescriptor);
        } finally {
            admin.close();
        }
    }
};
