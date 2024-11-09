package telran.pulse.monitoring;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import static telran.pulse.monitoring.Constants.*;
import org.json.JSONObject;

public class App {
    static DynamoDbClient clientDynamo = DynamoDbClient.builder().build();
    static Logger logger = Logger.getLogger("pulse-values-analyzer");
    
    private static final String BASE_URL = System.getenv("BASE_URL");
    static {
        loggerSetUp();
    }

    public void handleRequest(DynamodbEvent event, Context context) {
        var records = event.getRecords();
        if (records == null) {
            logger.severe("No records in the event");
        } else {
            records.forEach(r -> {
                var newImage = r.getDynamodb().getNewImage();
                if (newImage == null) {
                    logger.warning("No new image found");
                } else if ("INSERT".equals(r.getEventName())) {
                    processPulseValue(newImage);
                } else {
                    logger.warning(r.getEventName() + " event name but should be INSERT");
                }
            });
        }
    }

    private void processPulseValue(Map<String, AttributeValue> map) {
        int value = Integer.parseInt(map.get(VALUE_ATTRIBUTE).getN());
        String patientId = map.get(PATIENT_ID_ATTRIBUTE).getN();
        logger.finer(getLogMessage(map));

        Range range = fetchRangeForPatient(patientId);
        if (range != null && (value > range.max || value < range.min)) {
            logAndRecordAbnormalValue(map);
        }
    }

    private Range fetchRangeForPatient(String patientId) {
        HttpClient client = HttpClient.newHttpClient();
        String url = String.format("%s?patientId=%s", BASE_URL, patientId);
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                JSONObject json = new JSONObject(response.body());
                int min = json.getInt("min");
                int max = json.getInt("max");
                return new Range(min, max);
            } else {
                logger.warning("Failed to fetch range: " + response.body());
            }
        } catch (IOException | InterruptedException e) {
            logger.log(Level.SEVERE, "Error fetching range from range-provider", e);
        }
        return null;
    }

    private void logAndRecordAbnormalValue(Map<String, AttributeValue> map) {
        logger.info(getLogMessage(map));

        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> sdkMap = convertToSdkAttributeMap(map);
        clientDynamo.putItem(PutItemRequest.builder()
                .tableName(ABNORMAL_VALUES_TABLE_NAME)
                .item(sdkMap)
                .build());
    }

    private Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> convertToSdkAttributeMap(Map<String, AttributeValue> lambdaMap) {
        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> sdkMap = new HashMap<>();
        lambdaMap.forEach((key, value) -> {
            sdkMap.put(key, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(value.getN()).build());
        });
        return sdkMap;
    }

    private String getLogMessage(Map<String, AttributeValue> map) {
        return String.format("PatientId: %s, Value: %s", 
                map.get(PATIENT_ID_ATTRIBUTE).getN(), 
                map.get(VALUE_ATTRIBUTE).getN());
    }

    private static void loggerSetUp() {
        Level loggerLevel = Level.parse(System.getenv().getOrDefault(LOGGER_LEVEL_ENV_VARIABLE, DEFAULT_LOGGER_LEVEL));
        logger.setLevel(loggerLevel);
        Handler handler = new ConsoleHandler();
        handler.setLevel(Level.FINEST);
        logger.addHandler(handler);
    }

    record Range(int min, int max) {}
}