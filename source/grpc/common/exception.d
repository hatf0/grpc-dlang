module grpc.common.exception;
import core.exception;

/// client exception list - will do next. (idletimeout) 
/// 1 timeout
/// 2 neterror          
/// 3 data error

///  server tips:
///  tips no module
///  tips no method
///  tips don't support protocol. 

class gRPCException : Exception
{
    ///
    this(string msg, string file = __FILE__, size_t line = __LINE__)
    {
        super(msg, file, line);
    }
}

///
class gRPCTimeoutException : gRPCException
{
    ///
    this(string msg, string file = __FILE__, size_t line = __LINE__)
    {
        super(msg, file, line);
    }
}

///
class gRPCNetErrorException : gRPCException
{

    ///
    this(string msg, string file = __FILE__, size_t line = __LINE__)
    {
        super(msg, file, line);
    }
}

///
class gRPCDataErrorException : gRPCException
{
    ///
    this(string msg, string file = __FILE__, size_t line = __LINE__)
    {
        super(msg, file, line);
    }
}
