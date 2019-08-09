import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @ProjectName: hbase-demo
 * @Package: PACKAGE_NAME
 * @ClassName: FilterExampleTest
 * @Description:
 * @Author: yehui.mao
 * @CreateDate: 2019/7/28 15:40
 * @UpdateUser: yehui.mao
 */
public class FilterExampleTest {

    private static final String TABLE_NAME = "filter";

    @Before
    public void before() {

    }

    @Test
    public void rowFilterTest() {
//        FilterExample.rowFilter(TABLE_NAME, CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row-22")));
        FilterExample.rowFilter(TABLE_NAME, CompareOperator.EQUAL, new RegexStringComparator(".*-.2"));
//        FilterExample.rowFilter(TABLE_NAME, CompareOperator.EQUAL, new SubstringComparator("-5"));
    }

    @Test
    public void familyFilterTest() {
        FilterExample.familyFilter(TABLE_NAME);
    }

    @Test
    public void qualifierFilterTest() {
        FilterExample.qualifierFilter(TABLE_NAME);
    }

    @Test
    public void valueFilterTest() {
        FilterExample.valueFilter(TABLE_NAME);
    }

    @Test
    public void dependentColumnFilterTest() {
//        FilterExample.dependentColumnFilter(TABLE_NAME, true, CompareOperator.NO_OP, null);
        FilterExample.dependentColumnFilter(TABLE_NAME, false, CompareOperator.NO_OP, null);
//        FilterExample.dependentColumnFilter(TABLE_NAME, true, CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("q-55")));
//        FilterExample.dependentColumnFilter(TABLE_NAME, false, CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("q-55")));
//        FilterExample.dependentColumnFilter(TABLE_NAME, true, CompareOperator.EQUAL, new RegexStringComparator(".*5"));
//        FilterExample.dependentColumnFilter(TABLE_NAME, false, CompareOperator.EQUAL, new RegexStringComparator(".*5"));
    }

    @Test
    public void singleColumnValueFilterTest() {
        FilterExample.singleColumnValueFilter(TABLE_NAME);
    }

    @Test
    public void singleColumnValueExcludeFilterTest() {
        FilterExample.singleColumnValueExcludeFilter(TABLE_NAME);
    }

    @Test
    public void prefixFilterTest() {
        FilterExample.prefixFilter(TABLE_NAME);
    }

    @Test
    public void pageFilterTest() {
        FilterExample.pageFilter(TABLE_NAME);
    }

    @Test
    public void keyOnlyFilterTest() {
        FilterExample.keyOnlyFilter(TABLE_NAME);
    }

    @Test
    public void firstKeyOnlyFilterTest() {
        FilterExample.firstKeyOnlyFilter(TABLE_NAME);
    }

    @Test
    public void timestampsFilterTest() {
        FilterExample.timestampsFilter(TABLE_NAME);
    }

    @Test
    public void columnCountGetFilterTest() {
        FilterExample.columnCountGetFilter(TABLE_NAME);
    }

    @Test
    public void columnPaginationFilterTest() {
        FilterExample.columnPaginationFilter(TABLE_NAME);
    }

    @Test
    public void columnPrefixFilterTest() {
        FilterExample.columnPrefixFilter(TABLE_NAME);
    }

    @Test
    public void randomRowFilterTest() {
        FilterExample.randomRowFilter(TABLE_NAME);
    }

    @After
    public void after() {

    }

}
