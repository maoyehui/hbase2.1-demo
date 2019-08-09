import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @ProjectName: hbase-demo
 * @Package: PACKAGE_NAME
 * @ClassName: DecoratingFilterExample
 * @Description:
 * @Author: yehui.mao
 * @CreateDate: 2019/7/30 10:28
 * @UpdateUser: yehui.mao
 */
public class DecoratingFilterExampleTest {

    private static final String TABLE_NAME = "filter";

    @Before
    public void before() {

    }

    @Test
    public void skipFilterTest() {
        DecoratingFilterExample.skipFilter(TABLE_NAME);
    }

    @Test
    public void whileMatchFilterTest(){
        DecoratingFilterExample.whileMatchFilter(TABLE_NAME);
    }

    @After
    public void after() {

    }

}
