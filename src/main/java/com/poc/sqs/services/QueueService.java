package com.poc.sqs.services;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.poc.sqs.utils.QueueTypes;
import com.poc.sqs.utils.RequestDTO;

@Service
public class QueueService {

	@Value("${aws.accessKey}")
	private String awsAccessKey;

	@Value("${aws.secretKey}")
	private String awsSecretKey;

	public static String sqsUrl;

	public QueueService() {

	}

	public String createQueue(RequestDTO dto) {
		String queueUrl = null;
		String deadLetterQueueUrl = null;

		try {
			final AmazonSQS sqs = this.awsCredentialsProvider(dto.getAccessKey(), dto.getSecretKey());
			if (dto.getQueueType().equalsIgnoreCase(QueueTypes.STANDARD.name()) || dto.getQueueType() == null) {

				CreateQueueRequest createQueueRequest = new CreateQueueRequest(dto.getQueueName());

				CreateQueueResult flag = sqs.createQueue(createQueueRequest);
				queueUrl = flag.getQueueUrl();

			} else {
				final Map<String, String> attributes = new HashMap<>();

				// A FIFO queue must have the FifoQueue attribute set to true.
				attributes.put("FifoQueue", "true");

				/*
				 * If the user doesn't provide a MessageDeduplicationId, generate a
				 * MessageDeduplicationId based on the content.
				 */
				attributes.put("ContentBasedDeduplication", "true");

				final CreateQueueRequest createFIFOQueueRequest = new CreateQueueRequest(dto.getQueueName() + ".fifo")
						.withAttributes(attributes);
				queueUrl = sqs.createQueue(createFIFOQueueRequest).getQueueUrl();

				final CreateQueueRequest createDeadFIFOQueueRequest = new CreateQueueRequest(
						"DeadLetter" + dto.getQueueName() + ".fifo").withAttributes(attributes);
				deadLetterQueueUrl = sqs.createQueue(createDeadFIFOQueueRequest).getQueueUrl();

				final GetQueueAttributesResult deadLetterQueueAttributes = sqs.getQueueAttributes(
						new GetQueueAttributesRequest(deadLetterQueueUrl).withAttributeNames("QueueArn"));
				final String deadLetterQueueArn = deadLetterQueueAttributes.getAttributes().get("QueueArn");

				// Set the dead-letter queue for the source queue using the redrive policy.
				final SetQueueAttributesRequest request = new SetQueueAttributesRequest().withQueueUrl(queueUrl)
						.addAttributesEntry(QueueAttributeName.RedrivePolicy.toString(),
								"{\"maxReceiveCount\":\"5\", \"deadLetterTargetArn\":\"" + deadLetterQueueArn + "\"}");
				sqs.setQueueAttributes(request);
			}

		} catch (AmazonClientException e) {
			e.printStackTrace();
		}
		return queueUrl;
	}

	public AmazonSQS awsCredentialsProvider(String accessKey, String secretKey) {
		AmazonSQS amazonSQS = null;
		try {
			awsAccessKey = Objects.isNull(accessKey) ? awsAccessKey : accessKey;
			awsSecretKey = Objects.isNull(secretKey) ? awsSecretKey : secretKey;
			AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(
					new BasicAWSCredentials(awsAccessKey, awsSecretKey));
			amazonSQS = AmazonSQSClientBuilder.standard().withCredentials(awsCredentialsProvider).build();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return amazonSQS;
	}
}
