//package org.apache.hadoop.mytest_rpc.test2;
//
//import org.apache.hadoop.ipc.ProtobufRpcEngine;
//import org.apache.hadoop.ipc.RPC;
//
//public class server {
//    public static void main(String[] args) throws  Exception{
//
//
//        //1. 构建配置对象
//        Configuration conf = new Configuration();
//
//        //2. 协议对象的实例
//        MetaInfoServer serverImpl =  new MetaInfoServer();
//        BlockingService blockingService =
//                CustomProtos.MetaInfo.newReflectiveBlockingService(serverImpl);
//
//        //3. 设置协议的RpcEngine为ProtobufRpcEngine .
//        RPC.setProtocolEngine(conf, MetaInfoProtocol.class,
//                ProtobufRpcEngine.class);
//
//        //4. 构建RPC框架
//        RPC.Builder builder = new RPC.Builder(conf);
//        //5. 绑定地址
//        builder.setBindAddress("localhost");
//        //6. 绑定端口
//        builder.setPort(7777);
//        //7. 绑定协议
//        builder.setProtocol(MetaInfoProtocol.class);
//        //8. 调用协议实现类
//        builder.setInstance(blockingService);
//        //9. 创建服务
//        RPC.Server server = builder.build();
//        //10. 启动服务
//        server.start();
//
//    }
//}
