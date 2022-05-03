package app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigui.utils.MyDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.HDFSConstant;
import utils.KafkaConstant;
import utils.KafkaUtiles;
import utils.MySQLConstant;

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
 * @description:
 * @date: 2022-05-02 09:04
 */


public class RealToMySQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1 开启ck
        env.enableCheckpointing(100000L);
        env.setStateBackend(new FsStateBackend(HDFSConstant.CHECKPOINT_URI));
        // 设置超时
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // 设置失败后以一种怎样的方式重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5));

        // 设置kafka精确处理一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 任务取消时保留ck
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname(MySQLConstant.MYSQL_HOST)
                .port(MySQLConstant.PORT)
                .username(MySQLConstant.UERSR)
                .password(MySQLConstant.PASSWORD)
                .databaseList(MySQLConstant.DATABASES)
                .deserializer(new MyDeserializationSchema())
                .startupOptions(StartupOptions.latest()).build();
        //2 .添加数据源
        DataStreamSource<String> stringDataStreamSource = env.addSource(mysqlSource);
        stringDataStreamSource.print();

        //3. 同步到kafka

        stringDataStreamSource.addSink(KafkaUtiles.getKafkaProducter(KafkaConstant.ODS_BASE_DB));
        env.execute();

    }
}
