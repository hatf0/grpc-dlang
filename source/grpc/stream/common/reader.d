module grpc.stream.common.reader;

public import grpc.stream.common.stream;

import grpc.common.status : Status;

interface Reader {
    bool read(out ubyte[] buf);
    Status finish();
}


