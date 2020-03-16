package uci.edu.cs230.toy_cdn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import uci.edu.cs230.toy_cdn.fbs.FileExchangeHeader;

import java.nio.ByteBuffer;

/**
 * Push service will push any file instructed by the coordinator
 * */
public class PushService extends Thread {
    private final static Logger LOG = LogManager.getLogger(PushService.class);

    private ZContext mCtx;

    boolean InitializeOnly = false;

    // A PULL socket
    private ZMQ.Socket mSocketInternalLink;

    private EndPointAddress mExternalAddress;
    // A PUB socket
    private ZMQ.Socket mSocketPublisher;

    public PushService(ZContext context, EndPointAddress externalAddress) {
        mCtx = context;
        mExternalAddress = externalAddress;
    }

    private void init() {
        LOG.debug("Initializing PushService...");

        var syncCoordinator = mCtx.createSocket(SocketType.PAIR);
        syncCoordinator.bind(Common.EP_INT_SYNC_COORDINATOR_PUSH);
        // wait
        syncCoordinator.recv(0);

        mSocketInternalLink = mCtx.createSocket(SocketType.PULL);
        mSocketInternalLink.connect(Common.EP_INT_COORDINATOR);

        mSocketPublisher = mCtx.createSocket(SocketType.PUB);
        mSocketPublisher.bind(String.format("tcp://%s:%d", mExternalAddress.IpAddress, mExternalAddress.Port));
        LOG.debug("Setup PushService");

        // ping back
        syncCoordinator.send("READY", 0);
        syncCoordinator.close();
    }

    /**
     * Find out who's the recipient(s) by looking into the exchange header
     * */
    ZMsg handleExchangeMessage(ZMsg message) {
        // Action + ExchangeHeader
        assert message.size() >= 2;
        var frames = message.iterator();
        // action
        var action = frames.next().getString(ZMQ.CHARSET);
        switch (action.toUpperCase()) {
            case Common.EXG_ACTION_REQUEST: {
                // broadcast to all subscribers
                var broadcastMsg = message.duplicate();
                broadcastMsg.addFirst(Common.EXG_TOPIC_ALL);
                return broadcastMsg;
            }
            case Common.EXG_ACTION_RESPOND: {
                // find the next receiver
                var rawExchangeHeader = frames.next();
                var exchangeHeader = FileExchangeHeader.getRootAsFileExchangeHeader(ByteBuffer.wrap(rawExchangeHeader.getData()));
                var traceLen = exchangeHeader.traceLength();
                assert traceLen > 0;
                var nextStop = exchangeHeader.trace(traceLen - 1);
                var relayMsg = message.duplicate();
                // place subscription topic at the first frame
                relayMsg.addFirst(String.format("%d", nextStop.nodeId()));
                return relayMsg;
            }
            default:
                LOG.error(String.format("Unrecognized action \"%s\"", action));
        }

        return new ZMsg();
    }

    @Override
    public void run() {
        init();
        if(InitializeOnly) return;

        var poller = mCtx.createPoller(1);
        poller.register(mSocketInternalLink, ZMQ.Poller.POLLIN);

        while(!Thread.currentThread().isInterrupted()) {
            poller.poll();

            if(poller.pollin(0)) {
                var recvMsg = ZMsg.recvMsg(mSocketInternalLink);
                var replyMsg = handleExchangeMessage(recvMsg);
                recvMsg.destroy();
                if(replyMsg.size() < 1) {
                    LOG.error("Empty push message");
                    continue;
                }
                replyMsg.send(mSocketPublisher);
            }
        }
    }
}
