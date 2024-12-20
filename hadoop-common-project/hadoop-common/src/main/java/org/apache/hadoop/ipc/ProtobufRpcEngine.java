/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ipc;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.tracing.TraceScope;
import org.apache.hadoop.tracing.Tracer;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RPC Engine for for protobuf based RPCs.
 * This engine uses Protobuf 2.5.0. Recommended to upgrade to Protobuf 3.x
 * from hadoop-thirdparty and use ProtobufRpcEngine2.
 */
@Deprecated
@InterfaceStability.Evolving
public class ProtobufRpcEngine implements RpcEngine {
  public static final Logger LOG =
      LoggerFactory.getLogger(ProtobufRpcEngine.class);
  private static final ThreadLocal<AsyncGet<Message, Exception>>
      ASYNC_RETURN_MESSAGE = new ThreadLocal<>();

  static { // Register the rpcRequest deserializer for ProtobufRpcEngine
    //These will be used in server side, which is always ProtobufRpcEngine2
    ProtobufRpcEngine2.registerProtocolEngine();
  }

  private static final ClientCache CLIENTS = new ClientCache();

  @Unstable
  public static AsyncGet<Message, Exception> getAsyncReturnMessage() {
    return ASYNC_RETURN_MESSAGE.get();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      ConnectionId connId, Configuration conf, SocketFactory factory,
      AlignmentContext alignmentContext) throws IOException {
      //todo //构造一个实现了InvocationHandler接口的invoker 对象
      //    // (动态代理机制中的InvocationHandler对象会在invoke()方法中代理所有目标接口上的 调用，
      //    // 用户可以在invoke()方法中添加代理操作)
    final Invoker invoker = new Invoker(protocol, connId, conf, factory, alignmentContext);
    //todo     //然后调用Proxy.newProxylnstance()获取动态代理对象，并通过ProtocolProxy返回
      //todo loader ：       一个classloader对象，定义了由哪个classloader对象对生成的代理类进行加载
      //todo interfaces： 一个interface对象数组，表示我们将要给我们的代理对象提供一组什么样的接口，如果我们提供了这样一个接口对象数组，
      // 那么也就是声明了代理类实现了这些接口，代理类就可以调用接口中声明的所有方法。
      //todo h：一个InvocationHandler对象，表示的是当动态代理对象调用方法的时候会关联到哪一个InvocationHandler对象上，并最终由其调用。
    return new ProtocolProxy<T>(protocol, (T) Proxy.newProxyInstance(
        protocol.getClassLoader(), new Class[] {protocol}, invoker), false);
  }

  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout) throws IOException {
    return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
        rpcTimeout, null);
  }

  @Override
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy
      ) throws IOException {
    return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
      rpcTimeout, connectionRetryPolicy, null, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy,
      AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
      throws IOException {

    final Invoker invoker = new Invoker(protocol, addr, ticket, conf, factory,
        rpcTimeout, connectionRetryPolicy, fallbackToSimpleAuth,
        alignmentContext);
    return new ProtocolProxy<T>(protocol, (T) Proxy.newProxyInstance(
        protocol.getClassLoader(), new Class[]{protocol}, invoker), false);
  }
  
  @Override
  public ProtocolProxy<ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
      ConnectionId connId, Configuration conf, SocketFactory factory)
      throws IOException {
    Class<ProtocolMetaInfoPB> protocol = ProtocolMetaInfoPB.class;
    return new ProtocolProxy<ProtocolMetaInfoPB>(protocol,
        (ProtocolMetaInfoPB) Proxy.newProxyInstance(protocol.getClassLoader(),
            new Class[] { protocol }, new Invoker(protocol, connId, conf,
                factory, null)), false);
  }

  protected static class Invoker implements RpcInvocationHandler {
    private final Map<String, Message> returnTypes = 
        new ConcurrentHashMap<String, Message>();
    private boolean isClosed = false;
    private final Client.ConnectionId remoteId;
    private final Client client;
    private final long clientProtocolVersion;
    private final String protocolName;
    private AtomicBoolean fallbackToSimpleAuth;
    private AlignmentContext alignmentContext;

    protected Invoker(Class<?> protocol, InetSocketAddress addr,
        UserGroupInformation ticket, Configuration conf, SocketFactory factory,
        int rpcTimeout, RetryPolicy connectionRetryPolicy,
        AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
        throws IOException {
        //todo 调用getConnectionId方法 构建一个Client.ConnectionId对象.
      this(protocol, Client.ConnectionId.getConnectionId(
          addr, protocol, ticket, rpcTimeout, connectionRetryPolicy, conf),
          conf, factory, alignmentContext);
      this.fallbackToSimpleAuth = fallbackToSimpleAuth;
    }
    
    /**
     * This constructor takes a connectionId, instead of creating a new one.
     * @param protocol input protocol.
     * @param connId input connId.
     * @param conf input Configuration.
     * @param factory input factory.
     * @param alignmentContext Alignment context
     */
    //todo 这个是Invoker真正的构建方法,这里面会将刚刚构建好的ConnectionId 赋值给remoteId 字段.
    protected Invoker(Class<?> protocol, Client.ConnectionId connId,
        Configuration conf, SocketFactory factory, AlignmentContext alignmentContext) {
      this.remoteId = connId;
      //todo // 获取/创建  客户端
      this.client = CLIENTS.getClient(conf, factory, RpcWritable.Buffer.class);
      this.protocolName = RPC.getProtocolName(protocol);
      this.clientProtocolVersion = RPC
          .getProtocolVersion(protocol);
      this.alignmentContext = alignmentContext;
    }

    private RequestHeaderProto constructRpcRequestHeader(Method method) {
      RequestHeaderProto.Builder builder = RequestHeaderProto
          .newBuilder();
      builder.setMethodName(method.getName());
     

      // For protobuf, {@code protocol} used when creating client side proxy is
      // the interface extending BlockingInterface, which has the annotations 
      // such as ProtocolName etc.
      //
      // Using Method.getDeclaringClass(), as in WritableEngine to get at
      // the protocol interface will return BlockingInterface, from where 
      // the annotation ProtocolName and Version cannot be
      // obtained.
      //
      // Hence we simply use the protocol class used to create the proxy.
      // For PB this may limit the use of mixins on client side.
      builder.setDeclaringClassProtocolName(protocolName);
      builder.setClientProtocolVersion(clientProtocolVersion);
      return builder.build();
    }

    /**
     * This is the client side invoker of RPC method. It only throws
     * ServiceException, since the invocation proxy expects only
     * ServiceException to be thrown by the method in case protobuf service.
     * 
     * ServiceException has the following causes:
     * <ol>
     * <li>Exceptions encountered on the client side in this method are 
     * set as cause in ServiceException as is.</li>
     * <li>Exceptions from the server are wrapped in RemoteException and are
     * set as cause in ServiceException</li>
     * </ol>
     * 
     * Note that the client calling protobuf RPC methods, must handle
     * ServiceException by getting the cause from the ServiceException. If the
     * cause is RemoteException, then unwrap it to get the exception thrown by
     * the server.
     */
      /**
       * todo * ProtobufRpcEngine.Invoker.invoker() 方法主要做了三件事情:
       *      *  1.构造请求头域，
       *      *    使用protobuf将请求头序列化，这个请求头域 记录了当前RPC调用是什么接口的什么方法上的调用;
       *      *  2.通过RPC.Client类发送请求头以 及序列化好的请求参数。
       *      *    请求参数是在ClientNamenodeProtocolPB调用时就已经序列化好的，
       *      *    调用Client.call()方法时，
       *      *    需要将请求头以及请求参数使用一个RpcRequestWrapper对象封装;
       *      *  3.获取响应信息，序列化响应信息并返回。
       *
       * @return
       * @throws ServiceException
       */
    @Override
    public Message invoke(Object proxy, final Method method, Object[] args)
        throws ServiceException {
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = Time.now();
      }
      //todo       // pb接口的参数只有两个，即RpcController + Message
      if (args.length != 2) { // RpcController + Message
        throw new ServiceException(
            "Too many or few parameters for request. Method: ["
            + method.getName() + "]" + ", Expected: 2, Actual: "
            + args.length);
      }
      if (args[1] == null) {
        throw new ServiceException("null param while calling Method: ["
            + method.getName() + "]");
      }

      // if Tracing is on then start a new span for this rpc.
      // guard it in the if statement to make sure there isn't
      // any extra string manipulation.
      Tracer tracer = Tracer.curThreadTracer();
      TraceScope traceScope = null;
      if (tracer != null) {
        traceScope = tracer.newScope(RpcClientUtil.methodToTraceString(method));
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace(Thread.currentThread().getId() + ": Call -> " +
            remoteId + ": " + method.getName() +
            " {" + TextFormat.shortDebugString((Message) args[1]) + "}");
      }

        //todo       //获取请求调用的参数，例如RenameRequestProto
      final Message theRequest = (Message) args[1];
      final RpcWritable.Buffer val;
      try {
          //todo            //调用RPC.Client发送请求   constructRpcRequest //构造请求头域，标明在什么接口上调用什么方法
        val = (RpcWritable.Buffer) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
            constructRpcRequest(method, theRequest), remoteId,
            fallbackToSimpleAuth, alignmentContext);

      } catch (Throwable e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(Thread.currentThread().getId() + ": Exception <- " +
              remoteId + ": " + method.getName() +
                " {" + e + "}");
        }
        if (traceScope != null) {
          traceScope.addTimelineAnnotation("Call got exception: " +
              e.toString());
        }
        throw new ServiceException(e);
      } finally {
        if (traceScope != null) traceScope.close();
      }

      if (LOG.isDebugEnabled()) {
        long callTime = Time.now() - startTime;
        LOG.debug("Call: " + method.getName() + " took " + callTime + "ms");
      }
      
      if (Client.isAsynchronousMode()) {
        final AsyncGet<RpcWritable.Buffer, IOException> arr
            = Client.getAsyncRpcResponse();
        final AsyncGet<Message, Exception> asyncGet
            = new AsyncGet<Message, Exception>() {
          @Override
          public Message get(long timeout, TimeUnit unit) throws Exception {
            return getReturnMessage(method, arr.get(timeout, unit));
          }

          @Override
          public boolean isDone() {
            return arr.isDone();
          }
        };
        ASYNC_RETURN_MESSAGE.set(asyncGet);
        return null;
      } else {
        return getReturnMessage(method, val);
      }
    }

    protected Writable constructRpcRequest(Method method, Message theRequest) {
      RequestHeaderProto rpcRequestHeader = constructRpcRequestHeader(method);
      return new RpcProtobufRequest(rpcRequestHeader, theRequest);
    }

    private Message getReturnMessage(final Method method,
        final RpcWritable.Buffer buf) throws ServiceException {
      Message prototype = null;
      try {
        prototype = getReturnProtoType(method);
      } catch (Exception e) {
        throw new ServiceException(e);
      }
      Message returnMessage;
      try {
        returnMessage = buf.getValue(prototype.getDefaultInstanceForType());

        if (LOG.isTraceEnabled()) {
          LOG.trace(Thread.currentThread().getId() + ": Response <- " +
              remoteId + ": " + method.getName() +
                " {" + TextFormat.shortDebugString(returnMessage) + "}");
        }

      } catch (Throwable e) {
        throw new ServiceException(e);
      }
      return returnMessage;
    }

    @Override
    public void close() throws IOException {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    private Message getReturnProtoType(Method method) throws Exception {
      if (returnTypes.containsKey(method.getName())) {
        return returnTypes.get(method.getName());
      }
      
      Class<?> returnType = method.getReturnType();
      Method newInstMethod = returnType.getMethod("getDefaultInstance");
      newInstMethod.setAccessible(true);
      Message prototype = (Message) newInstMethod.invoke(null, (Object[]) null);
      returnTypes.put(method.getName(), prototype);
      return prototype;
    }

    @Override //RpcInvocationHandler
    public ConnectionId getConnectionId() {
      return remoteId;
    }

    protected long getClientProtocolVersion() {
      return clientProtocolVersion;
    }

    protected String getProtocolName() {
      return protocolName;
    }
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static Client getClient(Configuration conf) {
    return CLIENTS.getClient(conf, SocketFactory.getDefault(),
        RpcWritable.Buffer.class);
  }
//todo 在获取到ProtobufRpcEngine 之后, 调用其 getServer 方法, 获取Server 实例.

    /**
     * protocolClass : protocol协议的类
     * protocolImpl : protocol实现类
     * conf :  配置文件
     * bindAddress :  Server绑定的ip地址
     * port :   Server绑定的端口
     * numHandlers :  handler的线程数量 , 默认值 1
     * verbose :  是否每一个请求,都需要打印日志.
     * portRangeConfig  : A config parameter that can be used to restrict
     * alignmentContext :  provides server state info on client responses

     * @param protocol
     * @param protocolImpl
     * @param bindAddress
     * @param port
     * @param numHandlers
     * @param numReaders
     * @param queueSizePerHandler
     * @param verbose
     * @param conf
     * @param secretManager
     * @param portRangeConfig
     * @param alignmentContext
     * @return
     * @throws IOException
     */
  @Override
  public RPC.Server getServer(Class<?> protocol, Object protocolImpl,
      String bindAddress, int port, int numHandlers, int numReaders,
      int queueSizePerHandler, boolean verbose, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager,
      String portRangeConfig, AlignmentContext alignmentContext)
      throws IOException {
    return new Server(protocol, protocolImpl, conf, bindAddress, port,
        numHandlers, numReaders, queueSizePerHandler, verbose, secretManager,
        portRangeConfig, alignmentContext);
  }

  /**
   * Server implementation is always ProtobufRpcEngine2 based implementation,
   * supports backward compatibility for protobuf 2.5 based implementations,
   * which uses non-shaded protobuf classes.
   */
  public static class Server extends ProtobufRpcEngine2.Server {

    static final ThreadLocal<ProtobufRpcEngineCallback> currentCallback =
        new ThreadLocal<>();

    static class ProtobufRpcEngineCallbackImpl
        implements ProtobufRpcEngineCallback {

      private final RPC.Server server;
      private final Call call;
      private final String methodName;
      private final long setupTime;

      public ProtobufRpcEngineCallbackImpl() {
        this.server = CURRENT_CALL_INFO.get().getServer();
        this.call = Server.getCurCall().get();
        this.methodName = CURRENT_CALL_INFO.get().getMethodName();
        this.setupTime = Time.now();
      }

      @Override
      public void setResponse(Message message) {
        long processingTime = Time.now() - setupTime;
        call.setDeferredResponse(RpcWritable.wrap(message));
        server.updateDeferredMetrics(methodName, processingTime);
      }

      @Override
      public void error(Throwable t) {
        long processingTime = Time.now() - setupTime;
        String detailedMetricsName = t.getClass().getSimpleName();
        server.updateDeferredMetrics(detailedMetricsName, processingTime);
        call.setDeferredError(t);
      }
    }

    @InterfaceStability.Unstable
    public static ProtobufRpcEngineCallback registerForDeferredResponse() {
      ProtobufRpcEngineCallback callback = new ProtobufRpcEngineCallbackImpl();
      currentCallback.set(callback);
      return callback;
    }

    /**
     * Construct an RPC server.
     * 
     * @param protocolClass the class of protocol
     * @param protocolImpl the protocolImpl whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * @param portRangeConfig A config parameter that can be used to restrict
     * the range of ports used when port is 0 (an ephemeral port)
     * @param alignmentContext provides server state info on client responses
     * @param secretManager input secretManager.
     * @param queueSizePerHandler input queueSizePerHandler.
     * @param numReaders input numReaders.
     * @throws IOException raised on errors performing I/O.
     */
    //todo 首先会调用父类的构建方法.  然后再调用registerProtocolAndlmpl 方法注册接口类和接口的实现类
    public Server(Class<?> protocolClass, Object protocolImpl,
        Configuration conf, String bindAddress, int port, int numHandlers,
        int numReaders, int queueSizePerHandler, boolean verbose,
        SecretManager<? extends TokenIdentifier> secretManager, 
        String portRangeConfig, AlignmentContext alignmentContext)
        throws IOException {
      super(protocolClass, protocolImpl, conf, bindAddress, port, numHandlers,
          numReaders, queueSizePerHandler, verbose, secretManager,
          portRangeConfig, alignmentContext);
    }

    /**
     * This implementation is same as
     * ProtobufRpcEngine2.Server.ProtobufInvoker#call(..)
     * except this implementation uses non-shaded protobuf classes from legacy
     * protobuf version (default 2.5.0).
     */
    static RpcWritable processCall(RPC.Server server,
        String connectionProtocolName, RpcWritable.Buffer request,
        String methodName, ProtoClassProtoImpl protocolImpl) throws Exception {
      BlockingService service = (BlockingService) protocolImpl.protocolImpl;
      MethodDescriptor methodDescriptor = service.getDescriptorForType()
          .findMethodByName(methodName);
      if (methodDescriptor == null) {
        String msg = "Unknown method " + methodName + " called on "
                              + connectionProtocolName + " protocol.";
        LOG.warn(msg);
        throw new RpcNoSuchMethodException(msg);
      }
      Message prototype = service.getRequestPrototype(methodDescriptor);
      Message param = request.getValue(prototype);

      Message result;
      Call currentCall = Server.getCurCall().get();
      try {
        server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
        CURRENT_CALL_INFO.set(new CallInfo(server, methodName));
        currentCall.setDetailedMetricsName(methodName);
        result = service.callBlockingMethod(methodDescriptor, null, param);
        // Check if this needs to be a deferred response,
        // by checking the ThreadLocal callback being set
        if (currentCallback.get() != null) {
          currentCall.deferResponse();
          currentCallback.set(null);
          return null;
        }
      } catch (ServiceException e) {
        Exception exception = (Exception) e.getCause();
        currentCall
            .setDetailedMetricsName(exception.getClass().getSimpleName());
        throw (Exception) e.getCause();
      } catch (Exception e) {
        currentCall.setDetailedMetricsName(e.getClass().getSimpleName());
        throw e;
      } finally {
        CURRENT_CALL_INFO.set(null);
      }
      return RpcWritable.wrap(result);
    }
  }

  // htrace in the ipc layer creates the span name based on toString()
  // which uses the rpc header.  in the normal case we want to defer decoding
  // the rpc header until needed by the rpc engine.
  static class RpcProtobufRequest extends RpcWritable.Buffer {
    private volatile RequestHeaderProto requestHeader;
    private Message payload;

    public RpcProtobufRequest() {
    }

    RpcProtobufRequest(RequestHeaderProto header, Message payload) {
      this.requestHeader = header;
      this.payload = payload;
    }

    RequestHeaderProto getRequestHeader() throws IOException {
      if (getByteBuffer() != null && requestHeader == null) {
        requestHeader = getValue(RequestHeaderProto.getDefaultInstance());
      }
      return requestHeader;
    }

    @Override
    public void writeTo(ResponseBuffer out) throws IOException {
      requestHeader.writeDelimitedTo(out);
      if (payload != null) {
        payload.writeDelimitedTo(out);
      }
    }

    // this is used by htrace to name the span.
    @Override
    public String toString() {
      try {
        RequestHeaderProto header = getRequestHeader();
        return header.getDeclaringClassProtocolName() + "." +
               header.getMethodName();
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
