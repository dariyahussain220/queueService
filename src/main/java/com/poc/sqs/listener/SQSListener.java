package com.poc.sqs.listener;

import java.util.List;
import java.util.Objects;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.poc.sqs.services.QueueService;

@Component
public class SQSListener {

	@Value("${sqs.url}")
	private String sqsUrl;

	@Value("${aws.region}")
	private String awsRegion;

	private AmazonSQS amazonSQS;
	
	@Autowired
	private QueueService service;
	
	public SQSListener() {
		
	}

	private static final Logger logger = LoggerFactory.getLogger(SQSListener.class);

	@PostConstruct
	private void postConstructor() {

		if(Objects.isNull(QueueService.sqsUrl)) {
			QueueService.sqsUrl = sqsUrl;
		}
		logger.info("SQS URL: " + QueueService.sqsUrl);

		this.amazonSQS = service.awsCredentialsProvider(null, null);
	}

	public void startListeningToMessages() {

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(QueueService.sqsUrl).withMaxNumberOfMessages(1)
				.withWaitTimeSeconds(3);
		String previoussqsUrl = QueueService.sqsUrl;
		while (true) {
			
			if(!previoussqsUrl.equalsIgnoreCase(QueueService.sqsUrl)) {
				logger.info("SQS Changed? change listening request as well.");
				receiveMessageRequest = new ReceiveMessageRequest(QueueService.sqsUrl).withMaxNumberOfMessages(1)
						.withWaitTimeSeconds(3);
				previoussqsUrl = QueueService.sqsUrl;
			}
			logger.info("Trying to get message..");

			final List<Message> messages = amazonSQS.receiveMessage(receiveMessageRequest).getMessages();

			for (Message messageObject : messages) {
				String message = messageObject.getBody();

				logger.info("Received message: " + message);

				deleteMessage(messageObject);
			}
		}
	}
	

	private void deleteMessage(Message messageObject) {

		final String messageReceiptHandle = messageObject.getReceiptHandle();
		amazonSQS.deleteMessage(new DeleteMessageRequest(QueueService.sqsUrl, messageReceiptHandle));

	}

}
