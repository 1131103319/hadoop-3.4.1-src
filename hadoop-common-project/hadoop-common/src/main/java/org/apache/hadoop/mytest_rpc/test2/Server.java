package org.apache.hadoop.mytest_rpc.test2;


import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.RPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Server {

    public static final Logger LOG =
            LoggerFactory.getLogger(Server.class);


    // 接口信息 供server 和client 端使用
    @ProtocolInfo(
            protocolName = "org.apache.hadoop.rpc.CustomProtos$MetaInfoProtocol",
            protocolVersion = 1)
    public interface MetaInfoProtocol
            extends CustomProtos.MetaInfo.BlockingInterface {
    }

    // 实现类
    public static class MetaInfoServer implements MetaInfoProtocol {

        @Override
        public CustomProtos.GetMetaInfoResponseProto getMetaInfo(RpcController controller,
                                                                 CustomProtos.GetMetaInfoRequestProto request) throws
                ServiceException {

            //获取请求参数
            final String path = request.getPath();

            return CustomProtos.GetMetaInfoResponseProto.newBuilder().setInfo(path + ":3 - {BLOCK_1,BLOCK_2,BLOCK_3....").build();
        }
    }



    public static void main(String[] args) throws  Exception{

        //1. 构建配置对象
        Configuration conf = new Configuration();

        //2. 协议对象的实例
        MetaInfoServer serverImpl =  new MetaInfoServer();
        BlockingService blockingService =
                CustomProtos.MetaInfo.newReflectiveBlockingService(serverImpl);

        //3. 设置协议的RpcEngine为ProtobufRpcEngine .
        RPC.setProtocolEngine(conf, MetaInfoProtocol.class,
                ProtobufRpcEngine.class);

        //4. 构建RPC框架
        RPC.Builder builder = new RPC.Builder(conf);
        //5. 绑定地址
        builder.setBindAddress("localhost");
        //6. 绑定端口
        builder.setPort(7777);
        //7. 绑定协议
        builder.setProtocol(MetaInfoProtocol.class);
        //8. 调用协议实现类
        builder.setInstance(blockingService);
        builder.setVerbose(true);
        //9. 创建服务
        RPC.Server server = builder.build();
        //10. 启动服务
        server.start();

    }






}