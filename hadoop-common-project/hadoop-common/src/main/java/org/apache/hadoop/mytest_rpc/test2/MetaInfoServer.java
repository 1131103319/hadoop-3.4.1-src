package org.apache.hadoop.mytest_rpc.test2;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

// 实现类
public class MetaInfoServer implements MetaInfoProtocol {

    @Override
    public CustomProtos.GetMetaInfoResponseProto getMetaInfo(RpcController controller,
                                                             CustomProtos.GetMetaInfoRequestProto request) throws
            ServiceException {

        //获取请求参数
        final String path = request.getPath();

        return CustomProtos.GetMetaInfoResponseProto.newBuilder().setInfo(path + ":3 - {BLOCK_1,BLOCK_2,BLOCK_3....").build();
    }
}