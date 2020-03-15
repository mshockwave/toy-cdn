package uci.edu.cs230.toy_cdn;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import uci.edu.cs230.toy_cdn.registry.fbs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Main {
    private final static Logger LOG = LogManager.getLogger(Main.class);

    private static EndPointAddress getSelfAddress(Properties configProp) {
        var portStr = configProp.getProperty("cdn.port", "9487");
        var port = Integer.parseInt(portStr);
        var address = configProp.getProperty("cdn.service_address", "localhost");

        LOG.debug(String.format("Service will be available @ %s:%d", address, port));
        return new EndPointAddress(address, port);
    }

    private static List<EndPointAddress> fetchNeighborEndPoints(Properties configProp) {
        var registryAddr = configProp.getProperty("cdn.registry_address", "localhost");
        var registryPort = configProp.getProperty("cdn.registry_port", "4444");
        LOG.debug(String.format("Connecting to registry @ %s:%s ...", registryAddr, registryPort));

        try (var ctx = new ZContext()) {
            var client = ctx.createSocket(SocketType.REQ);
            if(!client.connect(String.format("tcp://%s:%s", registryAddr, registryPort))) {
                LOG.error("Failed to connect to registry");
                return null;
            }

            ZMsg reqZMsg = new ZMsg();
            var selfAddress = getSelfAddress(configProp);
            FlatBufferBuilder builder = new FlatBufferBuilder(0);
            var endpoint = EndPoint.createEndPoint(builder, builder.createString(selfAddress.IpAddress), selfAddress.Port);
            RegistrationReq.startRegistrationReq(builder);
            var latitude = Float.parseFloat(configProp.getProperty("cdn.latitude"));
            var longitude = Float.parseFloat(configProp.getProperty("cdn.longitude"));
            var location = GeoLocation.createGeoLocation(builder, latitude, longitude);
            RegistrationReq.addLocation(builder, location);
            RegistrationReq.addAddress(builder, endpoint);
            int request = RegistrationReq.endRegistrationReq(builder);
            builder.finish(request);
            reqZMsg.add(builder.sizedByteArray());

            reqZMsg.send(client);
            reqZMsg.destroy();
            var poller = ctx.createPoller(1);
            poller.register(client, ZMQ.Poller.POLLIN);
            poller.poll(1500);

            if(poller.pollin(0)) {
                var respZMsg = ZMsg.recvMsg(client);
                assert respZMsg.size() == 1;
                var frame = respZMsg.pop();
                assert frame.hasData();
                var rawData = frame.getData();
                var registrationResp = RegistrationResp.getRootAsRegistrationResp(ByteBuffer.wrap(rawData));
                if(registrationResp.status() == RegistrationStatus.OK) {
                    var endPoints = new ArrayList<EndPointAddress>();
                    for(int i = 0; i < registrationResp.neighborsLength(); ++i) {
                        var neighbor = registrationResp.neighbors(i);
                        endPoints.add(new EndPointAddress(neighbor.ipAddress(), neighbor.port()));
                    }
                    respZMsg.destroy();
                    poller.close();
                    return endPoints;
                } else {
                    LOG.error("Registration response error: " + registrationResp.status());
                }
                respZMsg.destroy();
            }

            poller.close();
            return null;
        }
    }

    public static void main(String[] args) {
        var configStream = Main.class.getClassLoader().getResourceAsStream("cdn.properties");
        assert configStream != null;
        var configProp = new Properties();
        try {
            configProp.load(configStream);
        } catch (IOException e) {
            LOG.error("Failed to read properties file");
            LOG.error(e);
            return;
        }

        /*
         * Step 1: Communicate with registry to figure out the neighbors
         * */
        var neighborEndPoints = fetchNeighborEndPoints(configProp);
        if(neighborEndPoints == null) {
            LOG.error("Failed to retrieve neighbor end points");
        } else {
            LOG.debug("Size of neighbors: " + neighborEndPoints.size());
        }
    }
}
