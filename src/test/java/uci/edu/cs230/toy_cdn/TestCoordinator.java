package uci.edu.cs230.toy_cdn;

import com.google.flatbuffers.FlatBufferBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMsg;
import uci.edu.cs230.toy_cdn.fbs.EndPoint;
import uci.edu.cs230.toy_cdn.fbs.FileExchangeHeader;
import uci.edu.cs230.toy_cdn.fbs.Subscription;
import uci.edu.cs230.toy_cdn.fbs.TraceNode;
import zmq.ZMQ;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public class TestCoordinator {
    private static class SimpleMockAnalysisService extends AbstractMockAnalysisService {
        public SimpleMockAnalysisService(ZContext ipcContext) {
            super(ipcContext);
        }

        @Override
        public void run() {
            init();
        }
    }
    private static class CDNServices {
        ZContext Ctx;

        Coordinator Core;
        PullService Pull;
        PushService Push;
        AbstractMockAnalysisService Analysis;

        public CDNServices(boolean initializeOnly) {
            Ctx = new ZContext();
            Core = new Coordinator(Ctx,
                    0, new EndPointAddress("localhost", 5555),
                    List.of());
            Core.InitializeOnly = initializeOnly;
            Pull = new PullService(Ctx, 0);
            Pull.InitializeOnly = initializeOnly;
            Push = new PushService(Ctx, new EndPointAddress("localhost", 5556));
            Push.InitializeOnly = initializeOnly;
        }

        public CDNServices(Coordinator.LocalStorageInterface localStorage) {
            Ctx = new ZContext();
            Core = new Coordinator(Ctx,
                    0, new EndPointAddress("localhost", 5555),
                    List.of(), localStorage);
            Pull = new PullService(Ctx, 0);
            Push = new PushService(Ctx, new EndPointAddress("localhost", 5556));
        }

        public void start() {
            Analysis.start();
            Pull.start();
            Push.start();
            Core.start();
        }

        public void joinAll() throws InterruptedException{
            Analysis.join();
            Pull.join();
            Push.join();
            Core.join();
        }
    }

    @Test
    public void testInitialization() {
        var cdn = new CDNServices(true);
        cdn.Analysis = new SimpleMockAnalysisService(cdn.Ctx);

        try {
            cdn.start();
            cdn.joinAll();
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
        var dummyAnalysis = new Coordinator.AnalysisQueryInterface() {

            @Override
            public boolean keep(String fileId) {
                return false;
            }
        };
        var dummyFileStorage = new Coordinator.LocalStorageInterface() {

            @Override
            public Optional<byte[]> fetchFile(String fileId) {
                return Optional.empty();
            }

            @Override
            public void putFile(String fileId, byte[] content) {
                Assert.assertTrue("File size larger than zero", content.length > 0);
            }
        };

        var respondHandler = new Coordinator.RespondHandler(87, dummyAnalysis, dummyFileStorage);

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
        Assert.assertEquals("Message size of 4", 4, relay.size());
        var actionHeader = relay.pop();
        Assert.assertEquals("RESPOND", actionHeader.getString(ZMQ.CHARSET));
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
        Assert.assertEquals("Message size of 3", 3, relay.size());
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
        Assert.assertEquals("Message size of 3", 3, relay.size());
        relay.pop();
        rawRelayHeader = relay.pop();
        relayHeader = FileExchangeHeader.getRootAsFileExchangeHeader(ByteBuffer.wrap(rawRelayHeader.getData()));
        Assert.assertEquals("file4", relayHeader.fileId());
        Assert.assertEquals(2, relayHeader.traceLength());
        Assert.assertEquals(9, relayHeader.trace(1).nodeId());
        relay.destroy();
    }

    @Test
    public void testRequestHandlerRelayAndHit() {
        var localStorage = new Coordinator.LocalStorageInterface() {
            @Override
            public Optional<byte[]> fetchFile(String fileId) {
                if(fileId.equals("file2")) return Optional.of("content2".getBytes());
                if(fileId.equals("file3")) return Optional.of("content3".getBytes());
                return Optional.empty();
            }

            @Override
            public void putFile(String fileId, byte[] content) { }
        };

        var requestHandler = new Coordinator.RequestHandler(94, localStorage);

        // Not hit. Relay instead
        var header1 = createExchangeHeader("file1",
                new TraceNodeStruct(0, 1),
                new TraceNodeStruct(1, 1),
                new TraceNodeStruct(87, 1));
        var msg = new ZMsg();
        msg.add(header1);
        var reply = requestHandler.onMessage(msg);
        msg.destroy();
        Assert.assertEquals("Message size of 2", 2, reply.size());
        var action = reply.pop();
        Assert.assertEquals("REQUEST", action.getString(ZMQ.CHARSET).toUpperCase());
        reply.destroy();

        var header2 = createExchangeHeader("file2",
                new TraceNodeStruct(3, 1));
        msg = new ZMsg();
        msg.add(header2);
        reply = requestHandler.onMessage(msg);
        msg.destroy();
        Assert.assertEquals("Message size of 3", 3, reply.size());
        action = reply.pop();
        Assert.assertEquals("RESPOND", action.getString(ZMQ.CHARSET).toUpperCase());
        // header
        reply.pop();
        var content = reply.pop();
        Assert.assertTrue(content.getString(ZMQ.CHARSET).contains("content2"));
        reply.destroy();

        var header3 = createExchangeHeader("file3",
                new TraceNodeStruct(87, 1),
                new TraceNodeStruct(65, 1));
        msg = new ZMsg();
        msg.add(header3);
        reply = requestHandler.onMessage(msg);
        msg.destroy();
        Assert.assertEquals("Message size of 3", 3, reply.size());
        action = reply.pop();
        Assert.assertEquals("RESPOND", action.getString(ZMQ.CHARSET).toUpperCase());
        // header
        reply.pop();
        content = reply.pop();
        Assert.assertTrue(content.getString(ZMQ.CHARSET).contains("content3"));
        reply.destroy();
    }

    @Test
    public void testHandShaking() {
        var cdn = new CDNServices(true);

        try {
            cdn.start();
            cdn.joinAll();

            final var coordinatorPort = 7777;

            var builder = new FlatBufferBuilder(0);
            var addressOffset = builder.createString("localhost");
            int endPointOffset  = EndPoint.createEndPoint(builder, addressOffset, coordinatorPort);
            builder.finish(endPointOffset);

            var handShakeMsg = new ZMsg();
            handShakeMsg.add(Common.EXG_ACTION_NEW_NEIGHBOR);
            handShakeMsg.add(builder.sizedByteArray());
            var outputMsg = cdn.Core.handleHandShaking(handShakeMsg);
            handShakeMsg.destroy();
            Assert.assertEquals(2, outputMsg.size());
            var action = outputMsg.popString();
            Assert.assertEquals(Common.INT_ACTION_SUBSCRIBE, action);
            var rawSubscription = outputMsg.pop();
            var subscription = Subscription.getRootAsSubscription(ByteBuffer.wrap(rawSubscription.getData()));
            Assert.assertEquals(1, subscription.endPointsLength());
            var endPoint = subscription.endPoints(0);
            // Port of PushService would be port of Coordinator + 1
            Assert.assertEquals(coordinatorPort + 1, endPoint.port());

            outputMsg.destroy();
        } catch (InterruptedException e) {
            Assert.fail("Interrupted");
        }
    }

    @Test
    public void testSimpleAnalysisCommunication() {
        class SimpleMessageMockAnalysisService extends AbstractMockAnalysisService {

            public SimpleMessageMockAnalysisService(ZContext ipcContext) {
                super(ipcContext);
            }

            @Override
            public void run(){
                init();
                try {
                    Thread.sleep(500);

                    {
                        var message = new ZMsg();
                        message.add("file1");
                        message.add("file2");
                        message.send(mSocketInternal);
                    }
                    Thread.sleep(500);

                    {
                        var message = new ZMsg();
                        message.add("file3");
                        message.add("file4");
                        message.send(mSocketInternal);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        class EmptyLocalStorage implements Coordinator.LocalStorageInterface {

            @Override
            public Optional<byte[]> fetchFile(String fileId) {
                return Optional.empty();
            }

            @Override
            public void putFile(String fileId, byte[] content) { }
        }

        var cdn = new CDNServices(new EmptyLocalStorage());
        cdn.Analysis = new SimpleMessageMockAnalysisService(cdn.Ctx);
        try {
            cdn.start();

            cdn.joinAll();
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
    }
}
