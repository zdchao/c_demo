package GroupedProcessingTimeWindowExample;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import redis.clients.jedis.Jedis;

public class GroupedProcessingTimeWindowExample {
public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<Tuple2<String,String>> stream = env
				.addSource(new RichParallelSourceFunction<Tuple2<String,String>>() {
					private volatile boolean running = true;
					public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
						while(true) {
							FileInputStream fileInputStream = new FileInputStream("/flink/a.txt");
							InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
							BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
							String txt = "";
							while((txt=bufferedReader.readLine())!=null) {
								String[] arr = txt.split(" ");
								ctx.collect(new Tuple2<String,String>(arr[0],arr[1]));
							}
							bufferedReader.close();
							inputStreamReader.close();
							fileInputStream.close();
							
							Thread.sleep(3000);
						}
					}
					public void cancel() {
						running = false;
					}
				});
		
		stream
			.keyBy(0)
			//.timeWindow(Time.seconds(2))
			.reduce(new SummingReducer())
			.addSink(new SinkFunction<Tuple2<String, String>>() {
				public void invoke(Tuple2<String, String> value) throws Exception {
					//System.out.println("output "+value.f0+":"+value.f1);
					
					try {
						File file = new File("/tmp/flink_out.txt");
						FileOutputStream fileOutputStream = new FileOutputStream(file,true);
						OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
						BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);
						bufferedWriter.write(value.f0.toString()+" "+value.f1.toString()+"\n");
						bufferedWriter.close();
						outputStreamWriter.close();
						fileOutputStream.close();
						Jedis jedis = new Jedis("127.0.0.1");
						jedis.set("txt","I'm redis txt.");
						jedis.close();
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
			});
		
		env.execute();
	}
	public static class SummingReducer implements ReduceFunction<Tuple2<String, String>> {

		public Tuple2<String, String> reduce(Tuple2<String, String> value1, Tuple2<String, String> value2) {
			return new Tuple2<String,String>(value1.f0,value2.f1);
		}
	}
}
