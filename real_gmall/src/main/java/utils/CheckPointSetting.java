package utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
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
 * @description:
 * @date: 2022-05-03 18:58
 */


public class CheckPointSetting {
    public static void getMyCheckPoint(StreamExecutionEnvironment env, int parallelism) {
        // 1.开启ck
        env.enableCheckpointing(10000L);
        //2. ck 超时
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        // 3.检查点模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 4.ck保存位置
        env.setStateBackend(new FsStateBackend(HDFSConstant.ODS_CHECKPOINT_URI));
        // 5.重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        System.setProperty("HADOOP_USER_NAME", "root");
        // 这是为kafka的分区数

        env.setParallelism(parallelism);
    }
}
