module grpc.server;
import grpc.service.info;

/* hunt lib imports */
import hunt.http.server.HttpServer : HttpServer;
import hunt.http.HttpRequest : HttpRequest;
import hunt.http.HttpResponse : HttpResponse;
import hunt.http.HttpFields;
import hunt.http.server.HttpServerOptions : HttpServerOptions;
import hunt.http.HttpVersion : HttpVersion;
import hunt.collection : HashMap, Map;
import hunt.util.Common : Callback;
import hunt.http.server.ServerSessionListener : ServerSessionListener;
import hunt.http.server.ServerHttpHandler : ServerHttpHandler, ServerHttpHandlerAdapter;
import grpc.common.status_code : StatusCode;
import grpc.common.status : Status;
import hunt.http.HttpMetaData : MetaData;

/* these three packages provide so much that it's a massive PiTA to document */
import hunt.http.codec.http.frame;
import hunt.http.codec.http.model;
import hunt.http.codec.http.stream;

import std.experimental.logger;

import std.conv : to;
import grpc.stream.common;

HeadersFrame endHeaderFrame(Status status, int streamId)
{
    HttpFields end_fileds = new HttpFields(2);
    int code = to!int(status.errorCode());
    end_fileds.add("grpc-status", to!string(code));
    end_fileds.add("grpc-message", status.errorMessage());
    return new HeadersFrame(streamId, new MetaData(HttpVersion.HTTP_2, end_fileds), null, true);
}

class Server {
@safe: 
    class gRPCServerSessionListener : ServerSessionListener {
        private {
            HttpServerOptions options;
            class gRPCServerListener : StreamListener {
                private {
                    gRPCStream _stream;
                }

                override void onHeaders(Stream stream, HeadersFrame frame) {
                    () @trusted { _stream.onHeaders(frame); }();

                }

                override StreamListener onPush(Stream stream, PushPromiseFrame frame) {
                    return null;
                }

                override void onData(Stream stream, DataFrame frame, Callback callback) {
                    () @trusted { tracef("onData (stream: %s, frame: %s)", stream, frame); }();

                    ubyte[] data = () @trusted { return _stream.parseAndMark(frame); }();


                }

                override void onReset(Stream stream, ResetFrame frame, Callback callback) {

                }

                override void onReset(Stream stream, ResetFrame frame) {

                }

                override bool onIdleTimeout(Stream stream, Exception x) {
                    return true;
                }

                override string toString() {
                    string ret;
                    () @trusted { 
                        ret = super.toString();
                    }();

                    return ret;
                }

                this(ref gRPCStream stream) @safe {
                    () @trusted { trace("instantiated a new StreamListener"); }();
                    _stream = stream;
                }
            }
        }
    
        override Map!(int, int) onPreface(Session session) {
            HashMap!(int, int) config;
            () @trusted {
                config = new HashMap!(int, int)();
                config.put(SettingsFrame.HEADER_TABLE_SIZE, 
                           options.getMaxDynamicTableSize());
                config.put(SettingsFrame.INITIAL_WINDOW_SIZE,
                           options.getInitialStreamSendWindow());

                tracef("onPreface: %s", session);
            }();


            return config;
        }

        override StreamListener onNewStream(Stream stream, HeadersFrame frame) {
            HttpRequest req;
            string path;

            () @trusted {
                req = cast(HttpRequest)frame.getMetaData();
                path = req.getURI().getPath();
                tracef("onNewStream (path): %s", path);
            }();

            bool matched = false;

            import std.string : split;
            string[] parts = path.split('/');
            if(parts.length < 2) {
                warningf("onNewStream: RPC failed to pass length validation, ignoring (got %d, expected 2)", parts.length);
                return null;
            }

            Method _method;

            foreach(method; methods) {
                if(method.name == path){ 
                    trace("onNewStream: matched method");
                    tracef("\tmethod name: %s, service name: %s", method.name, method.service);
                    _method = method;
                    matched = true;
                }
            }

            () @trusted {
                HttpFields fields = new HttpFields();
                auto response = new HttpResponse(HttpVersion.HTTP_2, 200, fields);
                auto responseHeader = new HeadersFrame(stream.getId(), response, null, false);
                stream.headers(responseHeader, Callback.NOOP);
            }();


            if(!matched) {
                () @trusted { 
                    Status status = new Status(StatusCode.NOT_FOUND, "gRPC: did not find method named \"" ~ parts[1] ~ "\"");
                    stream.headers(endHeaderFrame(status, stream.getId()), Callback.NOOP);
                }();
                return null;
            }

            auto client_stream = new gRPCStream(stream);
            auto listener = new gRPCServerListener(client_stream); 

            return listener;
        }

        override void onSettings(Session session, SettingsFrame frame) {

        }

        override void onPing(Session session, PingFrame frame) {

        }

        override void onReset(Session session, ResetFrame frame) {

        }

        override void onClose(Session session, GoAwayFrame frame) {

        }

        override void onClose(Session session, GoAwayFrame frame, Callback callback) {

        }

        override void onFailure(Session session, Exception failure) {

        }

        override void onFailure(Session session, Exception failure, Callback callback) {

        }

        override void onAccept(Session session) {

        }

        override bool onIdleTimeout(Session session) {
            return false;
        }

        override string toString() {
            string ret;
            () @trusted { 
                ret = super.toString();
            }();

            return ret;
        }

        this(HttpServerOptions _options) {
            options = _options;
        }
    }

    private {
        HttpServer _server;
        gRPCServerSessionListener _listener;
        Method[] methods;
        bool s_init;
        bool s_ready;
    }

    bool bind(string addr, int port) {
        HttpServerOptions options;

        () @trusted { 
            options = new HttpServerOptions(); 
            options.setSecureConnectionEnabled(false);
            options.setFlowControlStrategy("simple");
            options.setProtocol(HttpVersion.HTTP_2.asString());
            options.setPort(port);
            options.setHost(addr);
        }();

        () @trusted {
            _listener = new gRPCServerSessionListener(options);
            _server = new HttpServer(options, _listener, new ServerHttpHandlerAdapter(options), null); 
        }();

        
        return true;
    }

    void register(T)() 
    in { assert(s_init); assert(!s_ready); }
    do {
        import std.typecons;
        import std.traits;
        alias parent = BaseTypeTuple!T[1];
        alias serviceName = fullyQualifiedName!T;

        import google.rpc.status : RPC, ServerStreaming, ClientStreaming;

        pragma(msg, "gRPC ( " ~ serviceName ~ " )");
        pragma(msg, "\tinherited from: ", fullyQualifiedName!parent);

        static foreach(i, val; getSymbolsByUDA!(parent, RPC)) {
            () {
                Method _method;
                enum remoteName = getUDAs!(val, RPC)[0].methodName;
                import std.conv : to;
                pragma(msg, "RPC (" ~ to!string(i) ~ "): " ~ fullyQualifiedName!(val));
                pragma(msg, "\tRemote: " ~ remoteName);

                mixin("import " ~ moduleName!val ~ ";");

                static if(hasUDA!(val, ClientStreaming) && hasUDA!(val, ServerStreaming)) {
                    pragma(msg, "\tClient <- (stream) -> Server");
                    _method.clientStreaming = true;
                    _method.serverStreaming = true;
                }
                else static if(hasUDA!(val, ClientStreaming)) {
                    pragma(msg, "\tClient (stream) -> Server");
                    _method.clientStreaming = true;
                }
                else static if(hasUDA!(val, ServerStreaming)) {
                    pragma(msg, "\tClient <- (stream) Server");
                    _method.serverStreaming = true;
                }
                else {
                    pragma(msg, "\tClient <-> Server");
                }

                _method.service = serviceName;
                _method.name = remoteName;

                methods ~= _method; 
            }();
        }
    }
    
    void start() 
    in { assert(s_init); assert(!s_ready); }
    do {
        () @trusted { 
            _server.start();
        }();
    }

    void stop() 
    in { assert(s_init); assert(s_ready); }
    do {
        () @trusted {
            _server.stop();
        }();
    }

    this() {
        s_init = true;
    }
}
