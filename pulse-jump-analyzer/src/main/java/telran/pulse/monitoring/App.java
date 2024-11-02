package telran.pulse.monitoring;

import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;

public class App {
	public void handleRequest(DynamodbEvent event, Context context) {
		if (event.getRecords() != null && !event.getRecords().isEmpty()) {
			event.getRecords().forEach(r -> {
				Map<String, AttributeValue> map = r.getDynamodb().getNewImage();
				if (map == null) {
					System.out.println("No new image found");
				} else if (r.getEventName().equals("INSERT")) {
					System.out.printf("patientId=%s, timestamp=%s, pulseValue=%s\n",
							map.get("patientId").getN(), map.get("timestamp").getN(),
							map.get("value").getN());
				} else {
					System.out.println(r.getEventName());
				}
			});
		} else {
			System.out.println("No records found in the event.");
		}
	}
}
