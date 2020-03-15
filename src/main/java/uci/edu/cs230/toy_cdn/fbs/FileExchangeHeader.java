// automatically generated by the FlatBuffers compiler, do not modify

package uci.edu.cs230.toy_cdn.fbs;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class FileExchangeHeader extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_1_12_0(); }
  public static FileExchangeHeader getRootAsFileExchangeHeader(ByteBuffer _bb) { return getRootAsFileExchangeHeader(_bb, new FileExchangeHeader()); }
  public static FileExchangeHeader getRootAsFileExchangeHeader(ByteBuffer _bb, FileExchangeHeader obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public FileExchangeHeader __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public uci.edu.cs230.toy_cdn.fbs.TraceNode trace(int j) { return trace(new uci.edu.cs230.toy_cdn.fbs.TraceNode(), j); }
  public uci.edu.cs230.toy_cdn.fbs.TraceNode trace(uci.edu.cs230.toy_cdn.fbs.TraceNode obj, int j) { int o = __offset(4); return o != 0 ? obj.__assign(__vector(o) + j * 16, bb) : null; }
  public int traceLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public uci.edu.cs230.toy_cdn.fbs.TraceNode.Vector traceVector() { return traceVector(new uci.edu.cs230.toy_cdn.fbs.TraceNode.Vector()); }
  public uci.edu.cs230.toy_cdn.fbs.TraceNode.Vector traceVector(uci.edu.cs230.toy_cdn.fbs.TraceNode.Vector obj) { int o = __offset(4); return o != 0 ? obj.__assign(__vector(o), 16, bb) : null; }
  public String fileId() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer fileIdAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public ByteBuffer fileIdInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 6, 1); }

  public static int createFileExchangeHeader(FlatBufferBuilder builder,
      int traceOffset,
      int fileIdOffset) {
    builder.startTable(2);
    FileExchangeHeader.addFileId(builder, fileIdOffset);
    FileExchangeHeader.addTrace(builder, traceOffset);
    return FileExchangeHeader.endFileExchangeHeader(builder);
  }

  public static void startFileExchangeHeader(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addTrace(FlatBufferBuilder builder, int traceOffset) { builder.addOffset(0, traceOffset, 0); }
  public static void startTraceVector(FlatBufferBuilder builder, int numElems) { builder.startVector(16, numElems, 8); }
  public static void addFileId(FlatBufferBuilder builder, int fileIdOffset) { builder.addOffset(1, fileIdOffset, 0); }
  public static int endFileExchangeHeader(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }
  public static void finishFileExchangeHeaderBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedFileExchangeHeaderBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public FileExchangeHeader get(int j) { return get(new FileExchangeHeader(), j); }
    public FileExchangeHeader get(FileExchangeHeader obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}
