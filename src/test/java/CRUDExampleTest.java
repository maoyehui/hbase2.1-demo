import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @ProjectName: hbase-demo
 * @Package: PACKAGE_NAME
 * @ClassName: CURDExampleTest
 * @Description:
 * @Author: yehui.mao
 * @CreateDate: 2019/7/25 18:17
 * @UpdateUser: yehui.mao
 */
public class CRUDExampleTest {

    private static final String TABLE_NAME = "test";

    @Before
    public void before() {
//        CURDExample.create(TABLE_NAME);
        CRUDExample.put(TABLE_NAME, "1", "cf1", "name", "robbin");
        CRUDExample.put(TABLE_NAME, "2", "cf1", "name", "ruby");
        CRUDExample.put(TABLE_NAME, "3", "cf1", "gender", "male");
        CRUDExample.put(TABLE_NAME, "1", "cf1", "name", "kotlin");
        CRUDExample.store(TABLE_NAME);
    }

    @Test
    public void createTest() {
        CRUDExample.create(TABLE_NAME);
    }


    @Test
    public void deleteTest() {
        CRUDExample.delete(TABLE_NAME);
    }

    @Test
    public void putTest() {
        for (int i = 0; i < 50; i++) {
            CRUDExample.put(TABLE_NAME, "row-" + i, "cf1", "qualifier-1", "q-" + i);
        }
        CRUDExample.store(TABLE_NAME);
        CRUDExample.scan(TABLE_NAME);
    }

    @Test
    public void remove() {
        CRUDExample.remove(TABLE_NAME, "3");
    }

    @Test
    public void getTest() {
        CRUDExample.get(TABLE_NAME, "1");
        CRUDExample.get(TABLE_NAME, "1", 3);
    }

    @Test
    public void scanTest() {
        CRUDExample.scan(TABLE_NAME, 1, 1);
        CRUDExample.scan(TABLE_NAME, 4, 1);
        CRUDExample.scan(TABLE_NAME, 4, 2);
        CRUDExample.scan(TABLE_NAME, 2, 1);
        CRUDExample.scan(TABLE_NAME, 2, 2);
    }

    @After
    public void after() {

    }

}
