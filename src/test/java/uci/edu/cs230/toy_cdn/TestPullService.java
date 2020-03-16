package uci.edu.cs230.toy_cdn;

import com.google.flatbuffers.FlatBufferBuilder;
import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import uci.edu.cs230.toy_cdn.fbs.EndPoint;
import uci.edu.cs230.toy_cdn.fbs.Subscription;

public class TestPullService {
    private byte[] createSubscription(int numEndPoints, int startPort) {
        var builder = new FlatBufferBuilder(0);
        var endPoints = new int[numEndPoints];
        for(int i = 0; i < numEndPoints; ++i) {
            var addrOffset = builder.createString("localhost");
            endPoints[i] = EndPoint.createEndPoint(builder, addrOffset, startPort + i);
        }
        int endPointsOffset = Subscription.createEndPointsVector(builder, endPoints);
        Subscription.startSubscription(builder);
        Subscription.addEndPoints(builder, endPointsOffset);
        int subscription = Subscription.endSubscription(builder);
        builder.finish(subscription);
        return builder.sizedByteArray();
    }

    @Test
    public void testPullControl() throws InterruptedException {
        ZContext ctx = new ZContext();
        var pullService = new PullService(ctx, 9487);
        // initialize
        pullService.InitializeOnly = true;
        pullService.start();
        pullService.join();

        // Create dummy endpoint
        var publishers = new ZMQ.Socket[2];
        publishers[0] = ctx.createSocket(SocketType.PUB);
        publishers[0].bind("tcp://localhost:4444");
        publishers[1] = ctx.createSocket(SocketType.PUB);
        publishers[1].bind("tcp://localhost:4445");

        var rawSubscription = createSubscription(2, 4444);
        var subscriptionMsg = new ZMsg();
        subscriptionMsg.add("SUBSCRIBE");
        subscriptionMsg.add(rawSubscription);
        pullService.handlePullControl(subscriptionMsg);

        subscriptionMsg.destroy();
        for(var publisher : publishers) {
            publisher.close();
        }
    }
}
