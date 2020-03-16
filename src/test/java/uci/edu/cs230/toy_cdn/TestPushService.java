package uci.edu.cs230.toy_cdn;

import com.google.flatbuffers.FlatBufferBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMsg;
import uci.edu.cs230.toy_cdn.fbs.FileExchangeHeader;
import uci.edu.cs230.toy_cdn.fbs.TraceNode;

public class TestPushService {

    @Test
    public void testRequestMessageExchange() {
        ZContext ctx = new ZContext();
        var externalAddress = new EndPointAddress("localhost", 4444);
        var pushService = new PushService(ctx, externalAddress);

        var reqMsg = new ZMsg();
        reqMsg.add(Common.EXG_ACTION_REQUEST);
        reqMsg.add("doesn't matter");
        reqMsg.add("content");
        var outputMsg = pushService.handleExchangeMessage(reqMsg);
        reqMsg.destroy();
        Assert.assertEquals("Message size of 4", 4, outputMsg.size());
        Assert.assertEquals(Common.EXG_TOPIC_ALL, outputMsg.popString());
        Assert.assertEquals(Common.EXG_ACTION_REQUEST, outputMsg.popString());
        outputMsg.destroy();
    }

    @Test
    public void testRespondMessageExchange() {
        ZContext ctx = new ZContext();
        var externalAddress = new EndPointAddress("localhost", 4445);
        var pushService = new PushService(ctx, externalAddress);

        var respMsg = new ZMsg();
        respMsg.add(Common.EXG_ACTION_RESPOND);
        var builder = new FlatBufferBuilder(0);
        FileExchangeHeader.startTraceVector(builder, 2);
        // reverse order!!
        TraceNode.createTraceNode(builder, 87, 0);
        TraceNode.createTraceNode(builder, 94, 0);
        int traceOffset = builder.endVector();
        int fileIdOffset = builder.createString("file1");
        int exchangeHeader = FileExchangeHeader.createFileExchangeHeader(builder, traceOffset, fileIdOffset);
        builder.finish(exchangeHeader);
        respMsg.add(builder.sizedByteArray());
        respMsg.add("content");

        var outputMsg = pushService.handleExchangeMessage(respMsg);
        respMsg.destroy();
        Assert.assertEquals("Message size of 4", 4, outputMsg.size());
        Assert.assertEquals("87", outputMsg.popString());
        Assert.assertEquals(Common.EXG_ACTION_RESPOND, outputMsg.popString());
        outputMsg.destroy();
    }
}
