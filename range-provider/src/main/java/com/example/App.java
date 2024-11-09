package com.example;

import java.util.*;
import java.util.logging.Logger;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import org.json.JSONObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

public class App implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final String TABLE_NAME = System.getenv().getOrDefault("TABLE_NAME", "pulse_values");
    private static final Region DYNAMODB_REGION = Region.of(System.getenv().getOrDefault("DYNAMODB_REGION", "us-east-1"));

    private static final DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .region(DYNAMODB_REGION)
            .build();

    private static final Logger logger = Logger.getLogger(App.class.getName());
    private static Map<String, Range> ranges = new HashMap<>();

    static {
        if (isTableExists()) {
            loadPatientRanges();
        } else {
            logger.severe("Table " + TABLE_NAME + " does not exist in DynamoDB.");
        }
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");

        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent().withHeaders(headers);
        Map<String, String> mapParameters = input.getQueryStringParameters();

        try {
            if (!mapParameters.containsKey("patientId")) {
                throw new IllegalArgumentException("no patientId parameter");
            }

            String patientIdStr = mapParameters.get("patientId");
            Range range = ranges.get(patientIdStr);

            if (range == null) {
                throw new IllegalStateException(patientIdStr + " not found in ranges");
            }

            response.withStatusCode(200).withBody(getRangeJSON(range));
        } catch (IllegalArgumentException e) {
            response.withBody(getErrorJSON(e.getMessage())).withStatusCode(400);
        } catch (IllegalStateException e) {
            response.withBody(getErrorJSON(e.getMessage())).withStatusCode(404);
        }
        return response;
    }

    private static boolean isTableExists() {
        try {
            dynamoDbClient.describeTable(DescribeTableRequest.builder().tableName(TABLE_NAME).build());
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    private static void loadPatientRanges() {
        logger.info("Loading patient ranges from DynamoDB");

        ScanRequest scanRequest = ScanRequest.builder().tableName(TABLE_NAME).build();
        ScanResponse scanResponse = dynamoDbClient.scan(scanRequest);

        Map<String, List<Integer>> patientLowValues = new HashMap<>();
        Map<String, List<Integer>> patientHighValues = new HashMap<>();

        for (Map<String, AttributeValue> item : scanResponse.items()) {
            String patientId = item.get("patientId").n();
            int pulseValue = Integer.parseInt(item.get("value").n());

            if (pulseValue < 100) {
                patientLowValues.computeIfAbsent(patientId, k -> new ArrayList<>()).add(pulseValue);
            } else {
                patientHighValues.computeIfAbsent(patientId, k -> new ArrayList<>()).add(pulseValue);
            }
        }

        for (String patientId : patientLowValues.keySet()) {
            List<Integer> lowValues = patientLowValues.getOrDefault(patientId, Collections.emptyList());
            List<Integer> highValues = patientHighValues.getOrDefault(patientId, Collections.emptyList());

            int min = lowValues.isEmpty() ? 0 : (int) lowValues.stream().mapToInt(Integer::intValue).average().orElse(0);
            int max = highValues.isEmpty() ? 100 : (int) highValues.stream().mapToInt(Integer::intValue).average().orElse(100);

            ranges.put(patientId, new Range(min, max));
        }

        logger.info("Finished loading patient ranges");
    }

    private String getErrorJSON(String message) {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("error", message);
        return jsonObj.toString();
    }

    private String getRangeJSON(Range range) {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("min", range.min());
        jsonObj.put("max", range.max());
        return jsonObj.toString();
    }

    record Range(int min, int max) {}
}
