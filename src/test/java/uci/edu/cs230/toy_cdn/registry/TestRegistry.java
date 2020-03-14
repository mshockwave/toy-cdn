package uci.edu.cs230.toy_cdn.registry;

import com.google.flatbuffers.FlatBufferBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uci.edu.cs230.toy_cdn.registry.fbs.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TestRegistry {
    private List<byte[]> mRequests;

    private void createRequest(FlatBufferBuilder builder,
                              float latitude, float longitude, String ipAddress, int port) {
        builder.clear();
        var endpoint = EndPoint.createEndPoint(builder, builder.createString(ipAddress), port);
        RegistrationReq.startRegistrationReq(builder);
        var location = GeoLocation.createGeoLocation(builder, latitude, longitude);
        RegistrationReq.addLocation(builder, location);
        RegistrationReq.addAddress(builder, endpoint);
        int request = RegistrationReq.endRegistrationReq(builder);
        builder.finish(request);
    }

    @Before
    public void initArtifacts() {
        mRequests = new ArrayList<>();

        var builder = new FlatBufferBuilder(0);
        // DBH
        createRequest(builder, 33.6434162f, -117.8423849f, "192.168.0.2", 4444);
        mRequests.add(builder.sizedByteArray());
        // In-N-Out
        createRequest(builder, 33.6482452f, -117.8431137f, "192.168.0.4", 4343);
        mRequests.add(builder.sizedByteArray());
        // ARC
        createRequest(builder, 33.6436009f, -117.8275032f, "192.168.0.6", 4545);
        mRequests.add(builder.sizedByteArray());
        // Boston Public Library
        createRequest(builder, 42.3494025f, -71.0788386f, "192.168.0.8", 6666);
        mRequests.add(builder.sizedByteArray());
    }

    @Test
    public void testDuplicate() {
        RegistrationListener listener = new RegistrationListener();

        var rawResp = listener.onMessage(mRequests.get(0));
        var resp = RegistrationResp.getRootAsRegistrationResp(ByteBuffer.wrap(rawResp));
        Assert.assertEquals("Response Status", RegistrationStatus.OK, resp.status());

        rawResp = listener.onMessage(mRequests.get(0));
        resp = RegistrationResp.getRootAsRegistrationResp(ByteBuffer.wrap(rawResp));
        Assert.assertEquals("Response Status", RegistrationStatus.FAILED_DUPLICATE, resp.status());
    }

    @Test
    public void testIllFormat() {
        RegistrationListener listener = new RegistrationListener();

        var builder = new FlatBufferBuilder(0);
        createRequest(builder, 33.6436009f, -117.8275032f, "", 4545);
        var rawResp = listener.onMessage(builder.sizedByteArray());
        var resp = RegistrationResp.getRootAsRegistrationResp(ByteBuffer.wrap(rawResp));
        Assert.assertEquals("Response Status", RegistrationStatus.FAILED_INVALID_FORMAT, resp.status());

        builder.clear();
        createRequest(builder, 33.6436009f, -117.8275032f, "192.168.0.2", -87);
        rawResp = listener.onMessage(builder.sizedByteArray());
        resp = RegistrationResp.getRootAsRegistrationResp(ByteBuffer.wrap(rawResp));
        Assert.assertEquals("Response Status", RegistrationStatus.FAILED_INVALID_FORMAT, resp.status());
    }

    @Test
    public void testOrder1() {
        RegistrationListener listener = new RegistrationListener();

        var rawResp = listener.onMessage(mRequests.get(0));
        var resp = RegistrationResp.getRootAsRegistrationResp(ByteBuffer.wrap(rawResp));
        Assert.assertEquals("Response Status", RegistrationStatus.OK, resp.status());
        Assert.assertEquals("No Neighbor", 0, resp.neighborsLength());

        rawResp = listener.onMessage(mRequests.get(1));
        resp = RegistrationResp.getRootAsRegistrationResp(ByteBuffer.wrap(rawResp));
        Assert.assertEquals("Response Status", RegistrationStatus.OK, resp.status());
        Assert.assertEquals("One Neighbor", 1, resp.neighborsLength());

        rawResp = listener.onMessage(mRequests.get(2));
        resp = RegistrationResp.getRootAsRegistrationResp(ByteBuffer.wrap(rawResp));
        Assert.assertEquals("Response Status", RegistrationStatus.OK, resp.status());
        Assert.assertEquals("Two Neighbors", 2, resp.neighborsLength());

        rawResp = listener.onMessage(mRequests.get(3));
        resp = RegistrationResp.getRootAsRegistrationResp(ByteBuffer.wrap(rawResp));
        Assert.assertEquals("Response Status", RegistrationStatus.OK, resp.status());
        Assert.assertEquals("One Neighbor", 1, resp.neighborsLength());
    }
}
