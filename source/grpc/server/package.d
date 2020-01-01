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
import grpc.service;

HeadersFrame endHeaderFrame(Status status, int streamId)
{
    HttpFields end_fields = new HttpFields(2);
    int code = to!int(status.errorCode());
    end_fields.add("grpc-status", to!string(code));
    end_fields.add("grpc-message", status.errorMessage());
    return new HeadersFrame(streamId, new MetaData(HttpVersion.HTTP_2, end_fields), null, true);
}

extern(C) void handle_sigpipe(int) {
    // do nothing, we can safely ignore SIGPIPE because then our writes will just fail

}

class Server {
@safe: 
    class gRPCServerSessionListener : ServerSessionListener {
        private {
            HttpServerOptions options;

            /* 
               Represents a session established with the opening of a new stream 
               As well, this class receives all data about the stream from hunt, and then hands over/enqueues it 
               Basically, a 'shim' layer
             */

            class gRPCServerListener : StreamListener {
                private {
                    Method _method;
                    shared(gRPCStream) _stream;
                }

                override void onHeaders(Stream stream, HeadersFrame frame) {
                    () @trusted { _stream.onHeaders(frame); }();

                }

                override StreamListener onPush(Stream stream, PushPromiseFrame frame) {
                    return null;
                }

                override void onData(Stream stream, DataFrame frame, Callback callback) @trusted {
                    tracef("onData (stream: %s, frame: %s)", stream, frame);
                    _stream.onReceive(frame);

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

                this(ref shared gRPCStream stream, Method method) @safe {
                    () @trusted { 
                        tracef("instantiated a new StreamListener (method: %s, service: %s)", method.name, method.service); 
                        try {
                            services[method.service].dispatch(method.name, stream);
                        } catch(Exception e) {
                            errorf("could not spawn new handler? (error: %s)", e.msg);
                            assert(0);
                        }

                        tracef("spawned new service handler");
                    }();

                    _stream = stream;
                    _method = method;
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


            /* a gRPCServerListener represents a session that has been established */

            auto client_stream = new shared(gRPCStream)(stream);
            auto listener = new gRPCServerListener(client_stream, _method); 

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
            tracef("got close event");
        }

        override void onFailure(Session session, Exception failure) {

        }

        override void onFailure(Session session, Exception failure, Callback callback) {
            tracef("got fail");

        }

        override void onAccept(Session session) {
            tracef("accepted new client");

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
        ServiceHandler[string] services;
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
            () @trusted {
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
                services[serviceName] = new Service!T();
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
        import core.sys.posix.signal : bsd_signal, SIGPIPE;
        () @trusted {
            bsd_signal(SIGPIPE, &handle_sigpipe); //a SIGPIPE will occur when the pipe has broken (i.e client has disconnected, but we continue to write)
            // it's safe to ignore it in this case, because our write will just silently fail
        }();

        s_init = true;
    }
}
