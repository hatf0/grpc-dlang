module grpc.stream.server.writer;

import grpc.stream.common;

class ServerWriter(T) {
    private {
        shared gRPCStream _stream;
    }

    bool write(T obj) {
        return true;
    }

    bool finish() {
        return true;
    }

    this(ref shared gRPCStream stream) {
        _stream = stream;
    }
}

