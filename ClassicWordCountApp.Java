import org.apache.kafka.common.serialization.serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;


public class ClassicWordCountApp {
  
  public static void main(final String[] args) throws Exception{
    
    
    Properties appConfig = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "Classic-WordCount-App");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    
    
    KStreamBuilder streamBuilder = new KStreamBuilder();
    
    KStream<String, String> linesOfText = streamBuilder.stream("TopicForTextLines");
    KTable<String, Long> countsForWords = linesOfText.flatMapValues(lineOfText -> Arrays.asList(lineOfText.toLowerCase()
      .split("\\w+"))).groupBy((key, word)-> word).count("Counts");
    
    countsForWords.to(Serdes.String(), Serdes.Long(), "CountsOfWordsTopic");
    KafkaStreams appStreams = new KafkaStreams(streamBuilder, appConfig);
    appStreams.start();
    
    Runtime.getRuntime().addShutdownHook(new Thread(appStreams::close));
    
    
  } 
    
  }
  
  
  
  
  
  
  
  
  
  
  
}


