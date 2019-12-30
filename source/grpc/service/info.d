module grpc.service.info;

struct Method {
    string name;
    string service;
    bool clientStreaming;
    bool serverStreaming;
}

struct ServiceInfo {
    string name;
    Method[] methods;
};

