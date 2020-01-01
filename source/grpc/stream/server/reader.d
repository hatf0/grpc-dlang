module grpc.stream.server.reader;

import grpc.common.status;
import grpc.stream.common;
import core.time;

class ServerReader(T) {
    private {
        shared gRPCStream _stream;
    }

    bool read(out T obj, Duration timeout = 1.seconds) {
        T _obj;
        if(_stream.readObject!T(_obj, timeout)) {
            obj = _obj;
        }
        else {
            return false;
        }

        return true;
    }

    this(ref shared gRPCStream stream) {
        _stream = stream;
    }
}

