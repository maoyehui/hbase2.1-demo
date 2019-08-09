import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ProjectName: hbase-demo
 * @Package: PACKAGE_NAME
 * @ClassName: DecoratingFilterExample
 * @Description:
 * @Author: yehui.mao
 * @CreateDate: 2019/7/30 10:27
 * @UpdateUser: yehui.mao
 */
public class DecoratingFilterExample {

    public static Configuration conf = null;
    public static Connection conn;

    static {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "127.0.0.1");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.master", "127.0.0.1:60000");
            conf.set("hbase.client.write.buffer", "2097152");
            conn = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Skip Filter
     * 跳转过滤器：当过滤器发现某一行中的一列需要过滤时，那么整行数据都将被过滤掉。
     *
     * @param tableName
     */
    public static void skipFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Filter filter1 = new ValueFilter(CompareOperator.LESS, new BinaryComparator(Bytes.toBytes("q-12")));
            Scan scan1 = new Scan();
            scan1.setFilter(filter1);
            ResultScanner scanner1 = table.getScanner(scan1);
            for (Result result : scanner1) {
                System.out.println(result);
            }
            scanner1.close();
            System.out.println("------");

            Filter filter2 = new SkipFilter(filter1);
            Scan scan2 = new Scan();
            scan2.setFilter(filter2);
            ResultScanner scanner2 = table.getScanner(scan2);
            for (Result result : scanner2) {
                System.out.println(result);
            }
            scanner2.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * While Match Filter
     * 全匹配过滤器：当一条数据被过滤时，就会直接放弃这次扫描操作。
     *
     * @param tableName
     */
    public static void whileMatchFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Filter filter1 = new ValueFilter(CompareOperator.LESS, new BinaryComparator(Bytes.toBytes("q-12")));
            Scan scan1 = new Scan();
            scan1.setFilter(filter1);
            ResultScanner scanner1 = table.getScanner(scan1);
            for (Result result : scanner1) {
                System.out.println(result);
            }
            scanner1.close();
            System.out.println("------");

            Filter filter2 = new WhileMatchFilter(filter1);
            Scan scan2 = new Scan();
            scan2.setFilter(filter2);
            ResultScanner scanner2 = table.getScanner(scan2);
            for (Result result : scanner2) {
                System.out.println(result);
            }
            scanner2.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
