package cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * -                   _ooOoo_
 * -                  o8888888o
 * -                  88" . "88
 * -                  (| -_- |)
 * -                   O\ = /O
 * -               ____/`---'\____
 * -             .   ' \\| |// `.
 * -              / \\||| : |||// \
 * -            / _||||| -:- |||||- \
 * -              | | \\\ - /// | |
 * -            | \_| ''\---/'' | |
 * -             \ .-\__ `-` ___/-. /
 * -          ___`. .' /--.--\ `. . __
 * -       ."" '< `.___\_<|>_/___.' >'"".
 * -      | | : `- \`.;`\ _ /`;.`/ - ` : | |
 * -        \ \ `-. \_ __\ /__ _/ .-` / /
 * ======`-.____`-.___\_____/___.-`____.-'======
 * .............................................
 * -          佛祖保佑             永无BUG
 *
 * @author :LiangFangWei
 * @description:Flink_cdc 测试
 * @date: 2022-04-09 16:44
 */


public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启
        env.enableCheckpointing(5000L);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop100:9000/flinkCDC"));
        // checkpoint的时间超过5s则放弃本地checkpoint
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        // 手动取消任务时 保留ck
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DebeziumSourceFunction<String> mysqlSourceFunction = MySQLSource.<String>builder().hostname("11.0.0.100")
                .username("root")
                .password("123456")
                .databaseList("test")
                .tableList("test.cate_info")
                .deserializer(new MyDeserializationSchema())
                // initial: 先对表中的数据做一次快照 读完快照数据后 再读binlog最新位置的数据
                // latest:  不对表做快照，只读最新的binlog
                // earliest: 不对表做快照，从头开始读binlog
                .startupOptions(StartupOptions.initial()).build();
        DataStreamSource<String> stringDataStreamSource = env.addSource(mysqlSourceFunction);
        stringDataStreamSource.print();
        env.execute();

    }


    /**
     *
     *
     * SourceRecord{sourcePartition={server=mysql_binlog_source},
     * sourceOffset={file=mysql-bin.000009, pos=714}}
     * ConnectRecord{topic='mysql_binlog_source.test.cate_info',
     * kafkaPartition=null, key=null, keySchema=null,
     * value=Struct{after=Struct{id=1111,cate_title=蓝色的鱼,cate_subtitle=蓝色的鱼},
     * source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=last,db=test,table=cate_info,server_id=0,file=mysql-bin.000009,pos=714,row=0},op=c,ts_ms=1650703844348}, valueSchema=Schema{mysql_binlog_source.test.cate_info.Envelope:STRUCT},
     * timestamp=null, headers=ConnectHeaders(headers=)}
     */
}

