import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ProjectName: hbase-demo
 * @Package: PACKAGE_NAME
 * @ClassName: PutExample
 * @Description:
 * @Author: yehui.mao
 * @CreateDate: 2019/7/24 12:12
 * @UpdateUser: yehui.mao
 */
public class CRUDExample {
    private static final String TABLE_NAME = "test";
    private static final int BATCH_SIZE = 3177;
    private static List<Put> putList;

    public static Configuration conf = null;
    public static Connection conn;

    static {
        putList = new ArrayList<Put>();
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
     * create table
     */
    public static void create(String tableName, Compression.Algorithm compression) {
        try {
            Admin admin = conn.getAdmin();

            if (!admin.tableExists(TableName.valueOf(tableName))) {
                TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
                ColumnFamilyDescriptorBuilder columnDescBuilder = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes("cf1"))
                        .setBlocksize(32 * 1024)
                        .setCompressionType(compression)
                        .setDataBlockEncoding(DataBlockEncoding.NONE);
                tableDescBuilder.setColumnFamily(columnDescBuilder.build());
                admin.createTable(tableDescBuilder.build());
            } else {
                System.out.println("table exists");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void create(String tableName) {
        create(tableName, Compression.Algorithm.NONE);
    }

    /**
     * delete table
     */
    public static void delete(String tableName) {
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            if (admin != null) {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
            }
            admin.close();
            System.out.println("delete table: " + tableName + " done.");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * insert data
     */
    public static void put(String tableName, String rowKey, String family, String qualifier, String value) {
        try {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            put.setDurability(Durability.SKIP_WAL);
            putList.add(put);
            System.out.println("insert record " + rowKey + " to cache.");

            if (putList.size() == BATCH_SIZE) {
                Table table = null;
                try {
                    table = conn.getTable(TableName.valueOf(tableName));
                    table.put(putList);
                    System.out.println("insert records in cache to table " + tableName);
                    putList.clear();
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void store(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            table.put(putList);
            putList.clear();
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
     * delete record
     */
    public static void remove(String tableName, String rowKey) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            List list = new ArrayList();
            Delete del = new Delete(rowKey.getBytes());
            list.add(del);
            table.delete(list);
            System.out.println("delete record " + rowKey + " ok.");
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
     * query record
     */
    public static void get(String tableName, String rowKey, int versions) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            get.readVersions(versions);
            if (get.isCheckExistenceOnly()) {
                Result rs = table.get(get);
                for (Cell cell : rs.listCells()) {
                    System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println(cell.getTimestamp());
                    System.out.println("------");
                }
            } else {
                System.out.println("not found data according to rowkey");
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

    public static void get(String tableName, String rowKey) {
        get(tableName, rowKey, 1);
    }

    /**
     * scan table
     * 缓存是面向行一级的操作，而批量则是面向列一级的操作。
     *
     * @param tableName
     */
    public static void scan(String tableName, String startRow, String stopRow, int caching, int batch) {
        Table table = null;
        ResultScanner scanner = null;
        try {

            table = conn.getTable(TableName.valueOf(tableName));
            Scan s = new Scan();
            // 设置每次RPC请求行数，默认1
            s.setCaching(caching);
            // 设置每次RPC请求的列数，默认1
            s.setBatch(batch);
            s.setScanMetricsEnabled(true);
            if (startRow != null) {
                s.withStartRow(startRow.getBytes(), true);
            }
            if (stopRow != null) {
                s.withStopRow(stopRow.getBytes(), true);
            }

            scanner = table.getScanner(s);
            int counter = 0;
            for (Result result : scanner) {
                counter++;
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
            ScanMetrics scanMetrics = scanner.getScanMetrics();
            System.out.println("Caching: " + caching + " ,Batch: " + batch + " ,Result: " + counter + " ,RPCs: " + scanMetrics.countOfRPCcalls);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    public static void scan(String tableName) {
        scan(tableName, null, null, -1, -1);
    }

    public static void scan(String tableName, int caching, int batch) {
        scan(tableName, null, null, caching, batch);
    }

}
