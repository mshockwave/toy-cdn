package uci.edu.cs230.toy_cdn;

import org.junit.Assert;
import org.junit.Test;

public class TestLocalStorageAgent {
    @Test
    public void testSimplePutAndFetchFile() {
        var localStorage = new LocalStorageAgent(1);
        localStorage.putFile("file1", "hello world!".getBytes());
        var result = localStorage.fetchFile("file1");
        Assert.assertTrue(result.isPresent());
        Assert.assertTrue(result.get().length > 0);
    }
}
