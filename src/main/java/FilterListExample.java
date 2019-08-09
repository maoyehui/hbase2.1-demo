import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ProjectName: hbase-demo
 * @Package: PACKAGE_NAME
 * @ClassName: FilterListExample
 * @Description:
 * @Author: yehui.mao
 * @CreateDate: 2019/7/30 10:53
 * @UpdateUser: yehui.mao
 */
public class FilterListExample {

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
     * Filter List
     * FilterList可以设置多个过滤器共同限制返回到客户端的结果
     * 通过控制List中过滤器的顺序来进一步精确地控制过滤器的执行顺序
     *
     * @param tableName
     */
    public static void filterList(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            List<Filter> filters = new ArrayList<Filter>();

            Filter filter1 = new RowFilter(CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row-50")));
            filters.add(filter1);

            Filter filter2 = new RowFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row-60")));
            filters.add(filter2);

            Filter filter3 = new QualifierFilter(CompareOperator.EQUAL, new RegexStringComparator("qualifier-.*"));
            filters.add(filter3);

            FilterList filterList = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println(result);
            }
            scanner.close();
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
