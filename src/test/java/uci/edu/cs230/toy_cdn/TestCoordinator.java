package uci.edu.cs230.toy_cdn;

import com.google.flatbuffers.FlatBufferBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMsg;
import uci.edu.cs230.toy_cdn.fbs.FileExchangeHeader;
import uci.edu.cs230.toy_cdn.fbs.TraceNode;

import java.nio.ByteBuffer;

public class TestCoordinator {
    @Test
    public void testInitialization() {
        ZContext ctx = new ZContext();

        var coordinator = new Coordinator(ctx, 0);
        coordinator.InitializeOnly = true;
        var pullService = new PullService(ctx);
        pullService.InitializeOnly = true;
        var analysisEngine = new AnalysisEngineService(ctx);
        analysisEngine.InitializeOnly = true;

        try {
            analysisEngine.start();
            pullService.start();
            coordinator.start();

            pullService.join();
            analysisEngine.join();
            coordinator.join();
        }catch (InterruptedException e) {
            throw new AssertionError("Interrupted");
        }
    }

    private static class TraceNodeStruct {
        long NodeId;
        int Sequence;
        public TraceNodeStruct(long nodeId, int sequence) {
            NodeId = nodeId;
            Sequence = sequence;
        }
    }
    private byte[] createExchangeHeader(String fileId, TraceNodeStruct... trace) {
        var builder = new FlatBufferBuilder(0);
        var fileIdOffset = builder.createString(fileId);

        var traceLen = trace.length;
        FileExchangeHeader.startTraceVector(builder, traceLen);
        for(int i = traceLen - 1; i >= 0; --i) {
            var node = trace[i];
            TraceNode.createTraceNode(builder, node.NodeId, node.Sequence);
        }
        int traceOffset = builder.endVector();

        FileExchangeHeader.startFileExchangeHeader(builder);
        FileExchangeHeader.addTrace(builder, traceOffset);
        FileExchangeHeader.addFileId(builder, fileIdOffset);
        int exchangeHeader = FileExchangeHeader.endFileExchangeHeader(builder);
        builder.finish(exchangeHeader);

        return builder.sizedByteArray();
    }

    @Test
    public void testRespondHandlerRelayDuplicate() {
        var respondHandler = new Coordinator.RespondHandler(87);

        var header1 = createExchangeHeader("file1",
                new TraceNodeStruct(0, 1),
                new TraceNodeStruct(1, 1),
                new TraceNodeStruct(87, 1));
        var msg1 = new ZMsg();
        msg1.add(header1);
        msg1.add("content1-1");
        msg1.add("content1-2");
        var relay = respondHandler.onMessage(msg1);
        msg1.destroy();
        Assert.assertEquals("Message size of 3", 3, relay.size());
        var rawRelayHeader = relay.pop();
        var relayHeader = FileExchangeHeader.getRootAsFileExchangeHeader(ByteBuffer.wrap(rawRelayHeader.getData()));
        Assert.assertEquals("file1", relayHeader.fileId());
        Assert.assertEquals(2, relayHeader.traceLength());
        Assert.assertEquals(1, relayHeader.trace(1).nodeId());
        relay.destroy();

        // Duplicate with header1
        var header2 = createExchangeHeader("file2",
                new TraceNodeStruct(0, 1),
                new TraceNodeStruct(5, 1),
                new TraceNodeStruct(9, 1),
                new TraceNodeStruct(87, 1));
        var msg2 = new ZMsg();
        msg2.add(header2);
        msg2.add("content2-1");
        msg2.add("content2-2");
        relay = respondHandler.onMessage(msg2);
        msg2.destroy();
        Assert.assertEquals("Empty reply due to duplicate message", 0, relay.size());
        relay.destroy();

        // Not duplicate with header1
        var header3 = createExchangeHeader("file3",
                new TraceNodeStruct(0, 2),
                new TraceNodeStruct(5, 1),
                new TraceNodeStruct(9, 1),
                new TraceNodeStruct(87, 1));
        var msg3 = new ZMsg();
        msg3.add(header3);
        msg3.add("content3");
        relay = respondHandler.onMessage(msg3);
        msg3.destroy();
        Assert.assertEquals("Message size of 2", 2, relay.size());
        relay.destroy();

        // Not duplicate with header1
        var header4 = createExchangeHeader("file4",
                new TraceNodeStruct(7, 2),
                new TraceNodeStruct(9, 1),
                new TraceNodeStruct(87, 1));
        var msg4 = new ZMsg();
        msg4.add(header4);
        msg4.add("content4");
        relay = respondHandler.onMessage(msg4);
        msg4.destroy();
        Assert.assertEquals("Message size of 2", 2, relay.size());
        rawRelayHeader = relay.pop();
        relayHeader = FileExchangeHeader.getRootAsFileExchangeHeader(ByteBuffer.wrap(rawRelayHeader.getData()));
        Assert.assertEquals("file4", relayHeader.fileId());
        Assert.assertEquals(2, relayHeader.traceLength());
        Assert.assertEquals(9, relayHeader.trace(1).nodeId());
        relay.destroy();
    }
}
