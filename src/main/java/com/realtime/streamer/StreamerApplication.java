package com.realtime.streamer;

import com.realtime.streamer.topic.CreateTopic;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;


@SpringBootApplication
public class StreamerApplication {

	public static void main(String[] args) {
		SpringApplication.run(StreamerApplication.class, args);
	}

}
