module grpc.stream.common;
import hunt.http.codec.http.frame;
import hunt.http.codec.http.model;
import hunt.http.codec.http.stream;
import grpc.common.byte_buffer;
import hunt.collection.BufferUtils;
import std.experimental.logger;

class Iterator(T) {
}

class gRPCStream : GenericStream {
    private {
        Stream _stream;
        bool _ok = true;
        bool _eof = false;
        EvBuffer!ubyte _buf; 
        const ulong DATA_HEADER_LEN = 5;
    }

    @property bool ok() {
        return _ok;
    }

    @property bool async() {
        return true;
    }

    @property bool isClosed() {
        return _stream.isClosed();
    }

    void onHeaders(HeadersFrame frame) {

    }

    ubyte[] parseFrame(DataFrame frame) {
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

    void onReceive(DataFrame frame) {
        if(frame.isEndStream()) {
            _eof = true;
        }


        ubyte[] _body = parseFrame(frame);
        if(_body.length != 0) {
            trace("should add it to the list here");
        }
    }

    ubyte[] parseAndMark(DataFrame frame) {
        if(frame.isEndStream()) {
            tracef("frame (%s) is eof, marking", frame);
            _eof = true;
        }

        tracef("called parseAndMark on frame (%s)", frame);


        ubyte[] _body = parseFrame(frame);

        return _body;
    }

    bool writeData(ubyte[] data) {
        return true;
    }

    bool writeObject(T)(T obj) {
        return true;
    }

    ubyte[] readData() {
        ubyte[] buf;
        return buf;
    }

    bool readObject(T)(ref T obj) {
        return true;
    }

    bool finish() {
        return true;
    }

    @safe this(ref Stream stream) {
        _stream = stream;
        () @trusted { _buf = new EvBuffer!ubyte(); }();
    }

}



interface GenericStream {
    @property bool isClosed();
    @property bool ok();  
    @property bool async();

    void onHeaders(HeadersFrame frame);
    /* shared */

    /* parseFrame will read a full 'dataframe', and return it as a ubyte */

    /* shared */
    ubyte[] parseFrame(DataFrame frame);
    ubyte[] parseAndMark(DataFrame frame);

    /* shared */
    void onReceive(DataFrame frame);


    bool writeData(ubyte[] data);
    bool writeObject(T)(T obj);

    ubyte[] readData();
    bool readObject(T)(ref T obj);

    bool finish(); //finish writes
}

/*
class HTTP2Stream : GenericStream {


}
*/
