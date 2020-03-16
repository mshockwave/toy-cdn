// automatically generated by the FlatBuffers compiler, do not modify

package uci.edu.cs230.toy_cdn.fbs;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Subscription extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_1_12_0(); }
  public static Subscription getRootAsSubscription(ByteBuffer _bb) { return getRootAsSubscription(_bb, new Subscription()); }
  public static Subscription getRootAsSubscription(ByteBuffer _bb, Subscription obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Subscription __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public uci.edu.cs230.toy_cdn.fbs.EndPoint endPoints(int j) { return endPoints(new uci.edu.cs230.toy_cdn.fbs.EndPoint(), j); }
  public uci.edu.cs230.toy_cdn.fbs.EndPoint endPoints(uci.edu.cs230.toy_cdn.fbs.EndPoint obj, int j) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int endPointsLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public uci.edu.cs230.toy_cdn.fbs.EndPoint.Vector endPointsVector() { return endPointsVector(new uci.edu.cs230.toy_cdn.fbs.EndPoint.Vector()); }
  public uci.edu.cs230.toy_cdn.fbs.EndPoint.Vector endPointsVector(uci.edu.cs230.toy_cdn.fbs.EndPoint.Vector obj) { int o = __offset(4); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }

  public static int createSubscription(FlatBufferBuilder builder,
      int endPointsOffset) {
    builder.startTable(1);
    Subscription.addEndPoints(builder, endPointsOffset);
    return Subscription.endSubscription(builder);
  }

  public static void startSubscription(FlatBufferBuilder builder) { builder.startTable(1); }
  public static void addEndPoints(FlatBufferBuilder builder, int endPointsOffset) { builder.addOffset(0, endPointsOffset, 0); }
  public static int createEndPointsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startEndPointsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endSubscription(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }
  public static void finishSubscriptionBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedSubscriptionBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Subscription get(int j) { return get(new Subscription(), j); }
    public Subscription get(Subscription obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

