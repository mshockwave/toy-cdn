package uci.edu.cs230.toy_cdn;

import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZMsg;
import uci.edu.cs230.toy_cdn.fbs.FileExchangeHeader;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

public class TestAnalysisServiceAgent {

    private ZMsg createAnalysisMessage(String... fileIds) {
        var message = new ZMsg();
        Arrays.stream(fileIds).forEachOrdered(message::add);
        return message;
    }

    @Test
    public void testSingleRequest() {
        var messages = new ZMsg[] {
                createAnalysisMessage("file0", "file1", "file2"),
                createAnalysisMessage("file0", "file3", "file1"),
                createAnalysisMessage("file0", "file1", "file5"),
                createAnalysisMessage("file0", "file1", "file2"),
                createAnalysisMessage("file0", "file7", "file10"),
                createAnalysisMessage("file1", "file0", "file6"),
                createAnalysisMessage("file0", "file3", "file2"),
                createAnalysisMessage("file1", "file0", "file9"),
                createAnalysisMessage("file0", "file1", "file4"),
                createAnalysisMessage("file0", "file2", "file10"),
        };

        // None of the files exist
        var localStorage = new Coordinator.LocalStorageInterface() {

            @Override
            public Optional<byte[]> fetchFile(String fileId) {
                return Optional.empty();
            }

            @Override
            public void putFile(String fileId, byte[] content) { }
        };

        var analysisAgent = new AnalysisServiceAgent(0, localStorage);
        for(int i = 0; i < 9; ++i) {
            var outputMsg = analysisAgent.onMessage(messages[i]);
            Assert.assertEquals(String.format("Output of message %d should be empty", i),
                    0, outputMsg.size());
            outputMsg.destroy();
        }
        var outputMsg = analysisAgent.onMessage(messages[9]);
        Assert.assertEquals(2, outputMsg.size());
        Assert.assertEquals(Common.EXG_ACTION_REQUEST, outputMsg.popString());
        var rawRequestHeader = outputMsg.pop().getData();
        var requestHeader = FileExchangeHeader.getRootAsFileExchangeHeader(ByteBuffer.wrap(rawRequestHeader));
        Assert.assertEquals("file0", requestHeader.fileId());
        Assert.assertEquals(1, requestHeader.traceLength());
        Assert.assertEquals(0, requestHeader.trace(0).nodeId());
        Assert.assertEquals(0, requestHeader.trace(0).sequence());
        outputMsg.destroy();
    }
}
