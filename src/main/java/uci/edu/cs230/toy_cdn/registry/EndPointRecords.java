package uci.edu.cs230.toy_cdn.registry;

import com.grum.geocalc.Coordinate;
import com.grum.geocalc.EarthCalc;
import com.grum.geocalc.Point;
import uci.edu.cs230.toy_cdn.registry.fbs.RegistrationReq;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Carrying servers' geographic information
 * */
public class EndPointRecords {
    // Use a separated class in order to not keeping
    // reference on Flatbuffers message objects' underlying
    // data buffer.
    public static class EndPointRecord {
        public float Latitude, Longitude;
        public String IpAddress;
        public int Port;

        public EndPointRecord(float latitude, float longitude,
                              String ipAddress, int port) {
            Latitude = latitude;
            Longitude = longitude;
            IpAddress = ipAddress;
            Port = port;
        }

        // In kilometers
        public double distanceTo(EndPointRecord other) {
            var point1 = Point.at(
                    Coordinate.fromDegrees(Latitude),
                    Coordinate.fromDegrees(Longitude)
            );
            var point2 = Point.at(
                    Coordinate.fromDegrees(other.Latitude),
                    Coordinate.fromDegrees(other.Longitude)
            );
            return Math.abs(EarthCalc.gcdDistance(point1, point2)) / 1000.0f;
        }

        public static EndPointRecord from(RegistrationReq fbsReq) {
            var fbsEndPoint = fbsReq.address();
            var fbsLocation = fbsReq.location();
            return new EndPointRecord(
                    fbsLocation.latitude(), fbsLocation.longitude(),
                    fbsEndPoint.ipAddress(), fbsEndPoint.port()
            );
        }
    }

    private List<EndPointRecord> mEndPoints;

    public EndPointRecords() {
        mEndPoints = new ArrayList<>();
    }

    boolean containsEndPoint(EndPointRecord endPoint) {
        return mEndPoints.parallelStream()
                .anyMatch(record -> record.IpAddress.equals(endPoint.IpAddress) && record.Port == endPoint.Port);
    }

    public boolean containsEndPoint(RegistrationReq fbsReq) {
        return containsEndPoint(EndPointRecord.from(fbsReq));
    }

    void addEndPoint(EndPointRecord endPoint) {
        mEndPoints.add(endPoint);
    }

    public List<EndPointRecord> join(RegistrationReq fbsReq,
                                     double radiusKm, int minimumNeighbors) {
        var endPoint = EndPointRecord.from(fbsReq);
        return join(endPoint, radiusKm, minimumNeighbors);
    }

    List<EndPointRecord> join(EndPointRecord endPoint,
                              double radiusKm, int minimumNeighbors) {
        addEndPoint(endPoint);
        // Find k nearest neighbor in terms of geolocation distance
        var sorted = mEndPoints.stream()
                .filter(e -> e != endPoint)
                .sorted(Comparator.comparingDouble(endPoint::distanceTo))
                .collect(Collectors.toList());
        var neighbors = sorted.stream().limit(minimumNeighbors).collect(Collectors.toList());
        var extra = sorted.stream().skip(minimumNeighbors)
                .filter(record -> endPoint.distanceTo(record) < radiusKm)
                .collect(Collectors.toList());
        if(extra.size() > 0) {
            neighbors.addAll(extra);
        }
        return neighbors;
    }
}
