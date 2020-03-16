package uci.edu.cs230.toy_cdn;

import com.google.common.hash.Hashing;
import zmq.ZMQ;

public class Common {
    /**
     * EndPoint addresses
     */
    // Internal
    public static final String EP_INT_SYNC_COORDINATOR_PULL = "inproc://sync-coordinator-pull";
    public static final String EP_INT_SYNC_COORDINATOR_AE = "inproc://sync-coordinator-ae";
    public static final String EP_INT_COORDINATOR = "inproc://coordinator";
    public static final String EP_INT_PULL_SERVICE = "inproc://pull-service";
    public static final String EP_INT_PULL_CONTROL = "inproc://pull-control";
    public static final String EP_INT_PUSH_SERVICE = "inproc://push-service";
    public static final String EP_INT_ANALYSIS_ENGINE = "inproc://analysis-engine";

    @SuppressWarnings("UnstableApiUsage")
    public static long getNodeId(EndPointAddress address) {
        var hashFunc = Hashing.crc32();
        return hashFunc.newHasher()
                .putString(address.IpAddress, ZMQ.CHARSET)
                .putInt(address.Port)
                .hash().asLong();
    }
}
