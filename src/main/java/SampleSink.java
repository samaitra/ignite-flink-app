import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.sink.flink.IgniteSink;

import java.util.HashMap;
import java.util.Map;

public class SampleSink {

    public void run() {

        /** Cache name. */
        String TEST_CACHE = "myCache";
        long DFLT_STREAMING_EVENT = 10;
        final String GRID_CONF_FILE = "ignite.xml";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();
        IgniteSink igniteSink = null;
        igniteSink = new IgniteSink(TEST_CACHE, GRID_CONF_FILE);
        igniteSink.setAllowOverwrite(true);
        igniteSink.setAutoFlushFrequency(10);
        igniteSink.start();

        DataStream<Map> stream = env.addSource(new SourceFunction<Map>() {
            private boolean running = true;

            @Override
            public void run(SourceContext<Map> ctx) throws Exception {
                Map testDataMap = new HashMap<>();
                long cnt = 0;
                while (running && (cnt < DFLT_STREAMING_EVENT)) {
                    testDataMap.put(cnt, "ignite-" + cnt);
                    cnt++;
                }
                ctx.collect(testDataMap);
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).setParallelism(1);

        // sink data into the grid.
        stream.addSink(igniteSink);
        try {
                env.execute();
            } catch (Exception e){
            e.printStackTrace();
        }
        finally {
            igniteSink.stop();
        }
    }


    public static void main(String[] args) {
        new SampleSink().run();
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start();
        for (long i = 0; i < 10; i++)
            System.out.println(ignite.getOrCreateCache("myCache").get(i));

    }

}
