package com.realtime.streamer;

import com.realtime.streamer.topic.CreateTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class StreamerApplication {

	public static void main(String[] args) {
		SpringApplication.run(StreamerApplication.class, args);
		CreateTopic createTopic = new CreateTopic();
		createTopic.create();
	}

}
