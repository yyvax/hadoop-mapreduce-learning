import org.junit.Test;

import java.io.IOException;

public class HDFSUtilsTest {
    @Test
    public void testLs() throws IOException {
        HDFSUtils.ls("/");
    }
}
