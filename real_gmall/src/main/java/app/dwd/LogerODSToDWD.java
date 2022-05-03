package app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import utils.CheckPointSetting;
import utils.KafkaConstant;
import utils.KafkaUtiles;

import java.util.Locale;

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
 * @date: 2022-05-03 15:44
 * 实现功能:
 * 1.修正不是新用户 对于is_new=1的存储状态存储用户mid,如果之前没存储过 修改状态为1 否则就修改is_new=0
 * 2.分类日志
 * 日志根据启动和曝光 普通日志拆分到不同的kafka topic中
 */


public class LogerODSToDWD {
    public static void main(String[] args) throws Exception {
        //TODO: 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // CheckPointSetting.getMyCheckPoint(env, 3);

        // TODO:读取kafka数据
        String topic = "ods_base_log";
        DataStreamSource<String> dataStreamSource = env.addSource(KafkaUtiles.getKafkaConsumer(KafkaConstant.ODS_LOG_TOPIC, "ods_group_id"));
        OutputTag<String> mistakeOutputTag = new OutputTag<String>("mistake_log") {
        };

        OutputTag<String> startOutputTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {
        };
        // TODO: 每行数据转为json对象 错误的数据扔到侧输出流中
        SingleOutputStreamOperator<JSONObject> process = dataStreamSource.process(new ProcessFunction<String, JSONObject>() {


            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    // 错误的日志扔到侧输出流中
                    ctx.output(mistakeOutputTag, value);
                }
            }
        });

        // TODO: 判断新老用户

        SingleOutputStreamOperator<String> resultStream = process
                .keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, String>() {
            ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {

                valueState = getRuntimeContext().getState(new ValueStateDescriptor("midState", String.class));
                super.open(parameters);
            }

            /**
             *  如果是新用户 判断状态是否有 如果有 json对象修改为1 如果没有 状态修改为1
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                JSONObject common = value.getJSONObject("common");
                String isNew = common.getString("is_new");
                if ("1".equals(isNew)) {
                    if ("1".equals(valueState.value())) {
                        common.put("is_new", "0");
                    } else {
                        valueState.update("1");
                    }
                }
                // TODO: 分流
                // 启动日志写到输出侧输出流中
                String start = value.getString("start");
                JSONArray displays = value.getJSONArray("displays");

                if (start != null && start.length() > 0) {
                    ctx.output(startOutputTag, value.toString());
                    return;
                }
                if (displays != null && displays.size() > 0) {
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("common", common);
                    jsonObject.put("actions", value.getString("actions"));
                    jsonObject.put("page", value.getString("page"));
                    jsonObject.put("ts", value.getString("ts"));

                    for (int i = 0; i < displays.size(); i++) {
                        jsonObject.put("displays", displays.get(i));
                        ctx.output(displayOutputTag, jsonObject.toString());

                    }
                    return;

                }
                out.collect(value.toString());
            }
        });

        resultStream.getSideOutput(startOutputTag).addSink(KafkaUtiles.getKafkaProducter(KafkaConstant.DWD_SRART_LOG));
        resultStream.getSideOutput(displayOutputTag).addSink(KafkaUtiles.getKafkaProducter(KafkaConstant.DWD_DISPLAY_LOG));
        resultStream.addSink(KafkaUtiles.getKafkaProducter(KafkaConstant.DWD_PAGE_LOG));
        env.execute();

    }
}
