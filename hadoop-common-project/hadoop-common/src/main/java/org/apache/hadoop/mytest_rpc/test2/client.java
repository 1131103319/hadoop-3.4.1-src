//package org.apache.hadoop.mytest_rpc.test2;
//
//import org.apache.hadoop.ipc.ProtobufRpcEngine;
//import org.apache.hadoop.ipc.RPC;
//
//import java.net.InetSocketAddress;
//
//public class client {
//    public static void main(String[] args) throws Exception {
//
//        //1. 构建配置对象
//        Configuration conf = new Configuration();
//
//        //2. 设置协议的RpcEngine为ProtobufRpcEngine .
//        RPC.setProtocolEngine(conf, server.MetaInfoProtocol.class,
//                ProtobufRpcEngine.class);
//
//
//        //3. 拿到RPC协议
//        server.MetaInfoProtocol proxy = RPC.getProxy(server.MetaInfoProtocol.class, 1L,
//                new InetSocketAddress("localhost", 7777), conf);
//
//        //4. 发送请求
//        CustomProtos.GetMetaInfoRequestProto obj =  CustomProtos.GetMetaInfoRequestProto.newBuilder().setPath("/meta").build();
//
//        CustomProtos.GetMetaInfoResponseProto metaData = proxy.getMetaInfo(null, obj);
//
//        //5. 打印元数据
//        System.out.println(metaData.getInfo());
//
//    }
//}
