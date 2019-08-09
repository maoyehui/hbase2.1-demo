import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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
 * @ClassName: FilterExample
 * @Description:
 * @Author: yehui.mao
 * @CreateDate: 2019/7/28 15:11
 * @UpdateUser: yehui.mao
 */
public class FilterExample {

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
     * Row Filter
     * 行过滤器基于行键来过滤数据
     *
     * @param tableName
     * @param op
     * @param rowComparator
     */
    public static void rowFilter(String tableName, CompareOperator op, ByteArrayComparable rowComparator) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();

            Filter filter = new RowFilter(op, rowComparator);
            scan.setFilter(filter);
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

    /**
     * Family Filter
     * 列族过滤器基于列族来过滤数据
     *
     * @param tableName
     */
    public static void familyFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Filter filter = new FamilyFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("cf2")));

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println(result);
            }
            scanner.close();

            Get get = new Get(Bytes.toBytes("row-5"));
            get.setFilter(filter);
            Result result = table.get(get);
            System.out.println("Result of get():" + result);

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
     * Qualifier Filter
     * 列名过滤器基于列名来过滤数据
     *
     * @param tableName
     */
    public static void qualifierFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Filter filter = new QualifierFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("qualifier")));

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println(result);
            }
            scanner.close();

            Get get = new Get(Bytes.toBytes("row-5"));
            get.setFilter(filter);
            Result result = table.get(get);
            System.out.println(result);
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
     * Value Filter
     * 值过滤器用来筛选某个特定值的单元格，这种匹配只能使用EQUAL和NOT_EQUAL运算符
     *
     * @param tableName
     */
    public static void valueFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Filter filter = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("q-49"));

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
            }
            scanner.close();

            Get get = new Get(Bytes.toBytes("row-9"));
            get.setFilter(filter);
            Result result = table.get(get);
            if (result.size() > 0) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
            } else {
                System.out.println("Not found the cell with 'q-99'");
            }
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
     * Dependent Column Filter
     * 参考列过滤器允许用户指定一个参考列或引用列，并使用参考列控制其他列的过滤。
     * 参考列过滤器使用参考列的时间戳，并在过滤时包括所有与引用时间戳相同的列。
     *
     * @param tableName
     * @param dropDependentColumn
     * @param op
     * @param comparator
     */
    public static void dependentColumnFilter(String tableName, boolean dropDependentColumn, CompareOperator op, ByteArrayComparable comparator) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Filter filter;
            if (comparator != null) {
                filter = new DependentColumnFilter(Bytes.toBytes("cf1"), Bytes.toBytes("qualifier"), dropDependentColumn, op, comparator);
            } else {
                filter = new DependentColumnFilter(Bytes.toBytes("cf1"), Bytes.toBytes("qualifier"));
            }

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
            }
            scanner.close();

            Get get = new Get(Bytes.toBytes("row-55"));
            get.setFilter(filter);
            Result result = table.get(get);
            if (result.size() > 0) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
            }
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
     * Single Column Value Filter
     * 单列值过滤器用一列的值决定是否一行数据被过滤
     *
     * @param tableName
     */
    public static void singleColumnValueFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    Bytes.toBytes("cf1"),
                    Bytes.toBytes("qualifier"),
                    CompareOperator.EQUAL,
                    new BinaryComparator(Bytes.toBytes("q-4")));
            // 默认没有参考列的行是被包含在结果中的，用户可以使用setFilterIfMissing(true)来过滤这些行
            filter.setFilterIfMissing(true);
            // 使用setLatestVersionOnly(true)可以改变过滤器的行为，默认值为true，此时过滤器只检查参考列的最新版本，设为fasle之后会检查所有版本。
            filter.setLatestVersionOnly(true);

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
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

    /**
     * Single Column Value Exclude Filter
     * 单列值排除过滤器：参考列不被包含到结果中
     *
     * @param tableName
     */
    public static void singleColumnValueExcludeFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            SingleColumnValueFilter filter = new SingleColumnValueExcludeFilter(
                    Bytes.toBytes("cf1"),
                    Bytes.toBytes("qualifier"),
                    CompareOperator.EQUAL,
                    new BinaryComparator(Bytes.toBytes("q-4")));
            filter.setFilterIfMissing(true);
            filter.setLatestVersionOnly(true);

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
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

    /**
     * Prefix Filter
     * 前缀过滤器：在构造当前过滤器时传入一个行键前缀，所有与前缀匹配的行都会被返回到客户端
     * 扫描操作以字典序查找，当遇到比前缀大的行时，扫描操作就结束了
     *
     * @param tableName
     */
    public static void prefixFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Filter filter = new PrefixFilter(Bytes.toBytes("row-1"));

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
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

    /**
     * Page Filter
     * 分页过滤器对结果按行分页
     *
     * @param tableName
     */
    public static void pageFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Filter filter = new PageFilter(11);

            int totalRows = 0;
            byte[] lastRow = null;
            while (true) {
                Scan scan = new Scan();
                scan.setFilter(filter);
                if (lastRow != null) {
                    /*
                    HBase中的行键是按字典排序的，因此返回的结果也是如此排序的，并且起始行是被包括在结果中的。
                    用户需要拼接一个零字节（一个长度为零的字节数组）到之前的行键，这样可以保证最后返回的行在本轮扫描时不被包括。
                    零字节是最小的增幅。
                    */
                    byte[] postfix = Bytes.toBytes("postfix");
                    byte[] startRow = Bytes.add(lastRow, postfix);
                    System.out.println("start row: " + Bytes.toStringBinary(startRow));
                    scan.withStartRow(startRow, false);
                }
                ResultScanner scanner = table.getScanner(scan);
                int localRows = 0;
                for (Result result : scanner) {
                    System.out.println(localRows++ + ": " + result);
                    totalRows++;
                    lastRow = result.getRow();
                }
                scanner.close();
                if (localRows == 0) break;
            }
            System.out.println("total rows: " + totalRows);

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
     * Key Only Filter
     * 值返回键不返回值
     *
     * @param tableName
     */
    public static void keyOnlyFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            // 默认值为false，值被设为长度为0的字节数组；设置为true时，值被设为原值长度的字节数组
            Filter filter = new KeyOnlyFilter();

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
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

    /**
     * First Key Only Filter
     * 首次行键过滤器：访问一行中的第一列（HBase隐式排序）
     * 这种过滤器通常在行数统计的应用场景中使用。
     *
     * @param tableName
     */
    public static void firstKeyOnlyFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Filter filter = new FirstKeyOnlyFilter();

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
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

    /**
     * Timestamps Filter
     * 时间戳过滤器：在扫描结果中对版本进行细粒度的控制
     *
     * @param tableName
     */
    public static void timestampsFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            List<Long> ts = new ArrayList<Long>();
            ts.add(1564389119520L);
            Filter filter = new TimestampsFilter(ts);

            Scan scan = new Scan();
            scan.setFilter(filter);
            scan.setTimeRange(1564389119519L, 1564389119521L);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
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

    /**
     * Column Count Get Filter
     * 列计数过滤器：限制每行最多取回多少列
     *
     * @param tableName
     */
    public static void columnCountGetFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Filter filter = new ColumnCountGetFilter(1);

            Get get = new Get(Bytes.toBytes("row-5"));
            get.setFilter(filter);
            Result rs = table.get(get);
            for (Cell cell : rs.listCells()) {
                System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                System.out.println(cell.getTimestamp());
                System.out.println("------");
            }

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
     * Column Pagination Filter
     * 列分页过滤器：对一行的所有列进行分页
     *
     * @param tableName
     */
    public static void columnPaginationFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Filter filter = new ColumnPaginationFilter(5, 1);

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
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

    /**
     * Column Prefix Filter
     * 列前缀过滤器通过对列名称进行前缀匹配过滤。
     *
     * @param tableName
     */
    public static void columnPrefixFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Filter filter = new ColumnPrefixFilter(Bytes.toBytes("qualifier-"));

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
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

    /**
     * Random Row Filter
     * 随机行过滤器可以让结果中包含随机行。
     *
     * @param tableName
     */
    public static void randomRowFilter(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Filter filter = new RandomRowFilter(0.1F);

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
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
