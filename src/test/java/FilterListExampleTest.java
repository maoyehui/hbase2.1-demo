import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @ProjectName: hbase-demo
 * @Package: PACKAGE_NAME
 * @ClassName: FilterListExampleTest
 * @Description:
 * @Author: yehui.mao
 * @CreateDate: 2019/7/30 10:53
 * @UpdateUser: yehui.mao
 */
public class FilterListExampleTest {

    private static final String TABLE_NAME = "filter";

    @Before
    public void before() {

    }

    @Test
    public void filterListTest() {
        FilterListExample.filterList(TABLE_NAME);
    }

    @After
    public void after() {

    }

}
