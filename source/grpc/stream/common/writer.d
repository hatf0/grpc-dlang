module grpc.stream.common.writer;

public import grpc.stream.common;

import grpc.common.status;

interface Writer {
    bool write(ubyte[] msg, bool option = false);
    bool eof();
    Status finish();
}
    

