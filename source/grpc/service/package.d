module grpc.service;

import core.thread : Thread;
import grpc.stream.common;
import std.experimental.logger;
import core.time;

mixin template Reader(T) {
    static if(is(TemplateOf!T == void)) {
        alias input = T;
    }
    else {
        alias input = TemplateArgsOf!(T)[0];
    }

}

mixin template Writer(T) {
    static if(is(TemplateOf!T == void)) {
        alias output = T;
    }
    else {
        alias output = TemplateArgsOf!(T)[0];
    }
}

interface ServiceHandler {
    @safe void dispatch(string method, ref shared gRPCStream _stream);
}

import core.atomic;

class Service(T) : ServiceHandler {
    private {
        __gshared T service;
        void delegate(T, ref shared gRPCStream _stream)[string] _delegateTable;
        shared int activeThreads;
    }

    @safe class ServicerThread : Thread {
        this() {
            super(&run);
        }

        shared gRPCStream _stream; 
        string _method;
    private:
        void run() @trusted {
            tracef("servicer thread booting (method: %s)", _method);
            atomicOp!"+="(activeThreads, 1);

            try {
                _delegateTable[_method](service, _stream);
            } catch(Exception e) {
                errorf("delegate failed with error (msg: %s)", e.msg);
            }

            atomicOp!"-="(activeThreads, 1);
            tracef("servicer thread shutting down (method: %s)", _method);
            tracef("threads (active: %d)", atomicLoad(activeThreads));
        }
    }

    @safe void dispatch(string method, ref shared gRPCStream _stream) {
        tracef("dispatching new thread (active: %d)", activeThreads);
        ServicerThread _proc = new ServicerThread();
        if(_stream.isClosed()) {
            tracef("stream is closed, refusing to service");
            return;
        }

        _proc._stream = _stream;
        _proc._method = method;
        _proc.isDaemon = false;

        () @trusted {
            _proc.start();
        }();

    }

    this() {
        service = new T();

        import std.traits;
        import std.typecons;
        import google.rpc.status;
        alias parent = BaseTypeTuple!T[1];
        static foreach(i, val; getSymbolsByUDA!(parent, RPC)) {
            () {
                /*
                   The remote name (what is sent over the wire) is encoded
                   into the function with the help of the @RPC UDA.
                   This is part of the protobuf compiler, and gRPCD does
                   not require an additional pass with a gRPC compiler, as
                   UDAs eliminate the need for a second pass.
                */

                enum remoteName = getUDAs!(val, RPC)[0].methodName;
                mixin Reader!(Parameters!val[0]);

                mixin Writer!(Parameters!val[1]);
                _delegateTable[remoteName] = (T instance, ref shared gRPCStream stream) {
                    Status _callStatus;

                    import grpc.stream.server.reader;
                    import grpc.stream.server.writer;

                    ServerReader!(input) reader = new ServerReader!(input)(stream);
                    ServerWriter!(output) writer = new ServerWriter!(output)(stream);

                    static if(hasUDA!(val, ClientStreaming) == 0 && hasUDA!(val, ServerStreaming) == 0) {
                        trace("deserializing protobufs info");
                        input funcIn;

                        trace("reading input obj"); 
                        if(!reader.read(funcIn, 10.msecs)) { //this should be instant
                            error("could not read the input object");
                            return;
                        }

                        output funcOut;

                        mixin("_callStatus = instance." ~ __traits(identifier, val) ~ "(funcIn, funcOut);");
                    }
                    else static if(hasUDA!(val, ClientStreaming) && hasUDA!(val, ServerStreaming)) {

                    }
                    else static if(hasUDA!(val, ClientStreaming)) {

                    }
                    else static if(hasUDA!(val, ServerStreaming)) {

                    }

                    stream.finish(_callStatus);
                };
            }();
        }
    }
}


