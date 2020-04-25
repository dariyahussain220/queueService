package com.poc.sqs.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.poc.sqs.services.QueueService;
import com.poc.sqs.utils.QueueTypes;
import com.poc.sqs.utils.RequestDTO;
import com.poc.sqs.utils.UserResponse;

import io.swagger.annotations.Api;

@RestController("/api")
@Api
public class SQSController {

	private static final Logger log = LoggerFactory.getLogger(SQSController.class);

	@Autowired
	QueueService service;

	@PostMapping(value = "/sendMessage")
	public ResponseEntity<?> sendMessage(@RequestBody RequestDTO dto) {
		UserResponse response = new UserResponse();
		try {

			final AmazonSQS sqs = service.awsCredentialsProvider(dto.getAccessKey(), dto.getSecretKey());

			if (sqs != null) {

				try {
					GetQueueUrlResult getQueueURLResult = sqs.getQueueUrl(dto.getQueueName());
					QueueService.sqsUrl = getQueueURLResult.getQueueUrl();
				} catch (QueueDoesNotExistException e) {
					// TODO: handle exception
					e.printStackTrace();
					log.info("creating queue...");
					QueueService.sqsUrl = service.createQueue(dto);
				}
			}

			log.info("Sending a message.");
			SendMessageRequest sendMessageRequest = new SendMessageRequest(QueueService.sqsUrl, dto.getMessage());
			if(dto.getQueueType().equalsIgnoreCase(QueueTypes.FIFO.name())) {
				sendMessageRequest.setMessageGroupId("messageGroup1");
			}
			sqs.sendMessage(sendMessageRequest);
			

			log.info("Message Sent.\n");

		} catch (final AmazonServiceException ase) {
			log.error("Caught an AmazonServiceException, which means " + "your request made it to Amazon SQS, but was "
					+ "rejected with an error response for some reason.");
			log.error("Error Message:    " + ase.getMessage());
			log.error("HTTP Status Code: " + ase.getStatusCode());
			log.error("AWS Error Code:   " + ase.getErrorCode());
			log.error("Error Type:       " + ase.getErrorType());
			log.error("Request ID:       " + ase.getRequestId());
			response.setSuccess(false);
			response.setMessage(ase.getMessage());
			return new ResponseEntity<UserResponse>(response, HttpStatus.BAD_REQUEST);
		} catch (final AmazonClientException ace) {
			log.error("Caught an AmazonClientException, which means "
					+ "the client encountered a serious internal problem while "
					+ "trying to communicate with Amazon SQS, such as not " + "being able to access the network.");
			log.error("Error Message: " + ace.getMessage());
			response.setSuccess(false);
			response.setMessage(ace.getMessage());
			return new ResponseEntity<UserResponse>(response, HttpStatus.BAD_REQUEST);
		}
		response.setSuccess(true);
		response.setMessage("Message sent.");
		return new ResponseEntity<UserResponse>(response, HttpStatus.OK);
	}
}