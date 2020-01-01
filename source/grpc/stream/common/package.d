module grpc.stream.common;
import hunt.http.codec.http.frame;
import hunt.http.codec.http.model;
import hunt.http.codec.http.stream;
import grpc.common.byte_buffer;
import hunt.collection.BufferUtils;
import std.experimental.logger;
import grpc.common.queue;
import google.rpc.status;
import core.time;

const ulong DATA_HEADER_LEN = 5;

class gRPCStream : GenericStream {
    private {
        __gshared Stream _stream;
        bool _ok = true;
        bool _eof = false;
        __gshared Queue!(ubyte[]) _queue;
    }

    shared @safe @property bool ok() {
        return _ok;
    }

    static @property bool async() {
        return true;
    }

    shared @safe @property bool isClosed() {
        bool closed = () @trusted {return _stream.isClosed();}();
        return closed;
    }

    shared void onHeaders(HeadersFrame frame) {
    }

    shared ubyte[] parseFrame(DataFrame frame) {
        EvBuffer!ubyte _buf = new EvBuffer!ubyte();
        ubyte[] _body;

        if(frame !is null) {
            () @trusted {
                ubyte[] buf = cast(ubyte[])BufferUtils.toArray(frame.getData());

                _buf.mergeBuffer(buf);
            }();
            
            ulong bufLen;
            while(( bufLen = () @trusted {return _buf.getBufferLength();}() ) >= DATA_HEADER_LEN) {

                auto dataHead = new ubyte[DATA_HEADER_LEN];

                if(!_buf.copyOutFromHead(dataHead, DATA_HEADER_LEN)) {
                    break;
                }

                import std.bitmanip : bigEndianToNative;

                ulong bodyLen = bigEndianToNative!int(dataHead[1..5]);

                if(bodyLen > int.max || bodyLen < 0) {
                    _buf.reset();
                    break;
                }

                if(bufLen >= bodyLen + DATA_HEADER_LEN) {
                    if(!_buf.drainBufferFromHead(DATA_HEADER_LEN)) {
                        break;
                    }

                    if(bodyLen != 0) {
                        _body.length = bodyLen;
                        if(!_buf.removeBufferFromHead(_body, bodyLen)) {
                            break;
                        }
                    }
                }
                else {
                    break;
                }
            }

        }

        tracef("read body (%s)", _body);

        return _body;
    }

    shared void onReceive(DataFrame frame) {
        if(frame.isEndStream()) {
            _eof = true;
        }


        ubyte[] _body = parseFrame(frame);
        if(_body.length != 0) {
            _queue.put(_body);
        }
    }

    shared ubyte[] parseAndMark(DataFrame frame) {
        if(frame.isEndStream()) {
            tracef("frame (%s) is eof, marking", frame);
            _eof = true;
        }

        tracef("called parseAndMark on frame (%s)", frame);


        ubyte[] _body = parseFrame(frame);

        return _body;
    }

    shared bool writeData(ubyte[] data) {
        return true;
    }

    shared bool writeObject(T)(T obj) {
        return true;
    }

    shared ubyte[] readData(Duration timeout = 10.seconds) {
        ubyte[] buf;

        tracef("waiting for data from queue");
        if(_queue.empty()) {
            if(!_queue.notify(timeout)) {
                return buf;
            }
        }

        tracef("read data (data: %s)", buf);

        buf = _queue.front().dup();
        _queue.popFront();

        return buf;
    }

    shared bool readObject(T)(ref T obj, Duration timeout = 10.seconds) {
        ubyte[] buf = readData(timeout);

        if(buf.length == 0) {
            return false;
        }

        import google.protobuf;
        T deserializedObject;

        try {
            deserializedObject = buf.fromProtobuf!T();
        }
        catch(Exception e) {
            errorf("Deserialization fault (msg: %s)", e.msg);
            return false;
        }

        obj = deserializedObject; 
            
        return true;
    }

    shared bool finish(Status status) {
        import hunt.util.Common : Callback;
        import hunt.http.HttpFields;
        import hunt.http.HttpMetaData : MetaData;
        import hunt.http.HttpVersion : HttpVersion;
        import std.conv : to;

        if(isClosed()) {
            return false;
        }

        if(status.code != 0) {
            HttpFields end_fields = new HttpFields(2);
            int code = status.code;
            end_fields.add("grpc-status", to!string(code));
            end_fields.add("grpc-message", status.message);
            HeadersFrame frame = new HeadersFrame(_stream.getId(), new MetaData(HttpVersion.HTTP_2, end_fields), null, true); 
            _stream.headers(frame, Callback.NOOP);
        }

        return true;
    }

    @safe shared this(ref Stream stream) {
        () @trusted {
            _stream = stream;
            _queue = new Queue!(ubyte[])();
        }();
    }

    @safe this(ref Stream stream) {
        () @trusted {
            _stream = stream;
            _queue = new Queue!(ubyte[])();
        }();
    }

}

interface GenericStream {
    shared @property bool isClosed();
    shared @property bool ok();  
    static @property bool async();

    shared void onHeaders(HeadersFrame frame);
    /* shared */

    /* parseFrame will read a full 'dataframe', and return it as a ubyte */

    /* shared */
    shared ubyte[] parseFrame(DataFrame frame);
    shared ubyte[] parseAndMark(DataFrame frame);

    /* shared */
    shared void onReceive(DataFrame frame);


    shared bool writeData(ubyte[] data);
    shared bool writeObject(T)(T obj);

    shared ubyte[] readData(Duration timeout);
    shared bool readObject(T)(ref T obj, Duration timeout);

    shared bool finish(Status status); //finish writes
}

/*
class HTTP2Stream : GenericStream {


}
*/
