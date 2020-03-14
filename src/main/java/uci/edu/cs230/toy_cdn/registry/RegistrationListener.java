package uci.edu.cs230.toy_cdn.registry;

import com.google.flatbuffers.FlatBufferBuilder;
import uci.edu.cs230.toy_cdn.StatelessMessageListener;
import uci.edu.cs230.toy_cdn.registry.fbs.EndPoint;
import uci.edu.cs230.toy_cdn.registry.fbs.RegistrationReq;
import uci.edu.cs230.toy_cdn.registry.fbs.RegistrationResp;
import uci.edu.cs230.toy_cdn.registry.fbs.RegistrationStatus;

import java.nio.ByteBuffer;
import java.util.Objects;

public class RegistrationListener implements StatelessMessageListener {
    private FlatBufferBuilder mRespBuilder;
    private EndPointRecords mEndPointRepo;

    /**
     * Each endpoint will connect to any other endpoints within this radius.
     */
    private static final float NEIGHBOR_RADIUS_KM = 15.0f;
    /**
     * Minimum neighbor(s) a endpoint need to connect to
     * */
    private static final int NUM_MINIMUM_NEIGHBOR = 1;

    public RegistrationListener() {
        mRespBuilder = new FlatBufferBuilder(0);
        mEndPointRepo = new EndPointRecords();
    }

    private byte[] getFailedResponse(byte status) {
        mRespBuilder.clear();
        // Empty neighbors
        var neighbors = RegistrationResp.createNeighborsVector(mRespBuilder, new int[0]);
        RegistrationResp.startRegistrationResp(mRespBuilder);
        RegistrationResp.addStatus(mRespBuilder, status);
        RegistrationResp.addNeighbors(mRespBuilder, neighbors);
        int resp = RegistrationResp.endRegistrationResp(mRespBuilder);
        mRespBuilder.finish(resp);
        return mRespBuilder.sizedByteArray();
    }

    public byte[] onMessage(byte[] message) {
        var inMsgBuffer = ByteBuffer.wrap(message);
        var request = RegistrationReq.getRootAsRegistrationReq(inMsgBuffer);

        // Sanity checks
        if(Objects.requireNonNull(request.address().ipAddress()).length() == 0
            || request.address().port() <= 0) {
            return getFailedResponse(RegistrationStatus.FAILED_INVALID_FORMAT);
        }
        if(mEndPointRepo.containsEndPoint(request)) {
            return getFailedResponse(RegistrationStatus.FAILED_DUPLICATE);
        }

        var neighborEndPoints = mEndPointRepo.join(request, NEIGHBOR_RADIUS_KM, NUM_MINIMUM_NEIGHBOR);

        mRespBuilder.clear();
        // Create neighbor vector
        var neighbors = new int[neighborEndPoints.size()];
        for(int i = 0; i < neighbors.length; ++i) {
            var endPoint = neighborEndPoints.get(i);
            int addr = mRespBuilder.createString(endPoint.IpAddress);
            int port = endPoint.Port;
            neighbors[i] = EndPoint.createEndPoint(mRespBuilder, addr, port);
        }
        int neighborVec = RegistrationResp.createNeighborsVector(mRespBuilder, neighbors);

        RegistrationResp.startRegistrationResp(mRespBuilder);
        RegistrationResp.addNeighbors(mRespBuilder, neighborVec);
        RegistrationResp.addStatus(mRespBuilder, RegistrationStatus.OK);
        int response = RegistrationResp.endRegistrationResp(mRespBuilder);
        mRespBuilder.finish(response);

        return mRespBuilder.sizedByteArray();
    }
}
