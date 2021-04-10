package com.berlin.cse591;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.net.URISyntaxException;

import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;

@SpringBootApplication
public class Cse591Application {

	public static void main(String[] args) {
		SpringApplication.run(Cse591Application.class, args);
	}

	public void connect() throws IOException, IllegalArgumentException, URISyntaxException {
		System.out.println("Starting...");
		System.out.println("Beginning setup.");

		String connString = "conn string here";
		IotHubClientProtocol protocol = IotHubClientProtocol.MQTT;

		DeviceClient client = new DeviceClient(connString, protocol);

		System.out.println("Successfully created an IoT Hub client.");

		MessageCallbackMqtt callback = new MessageCallbackMqtt();
		Counter counter = new Counter(0);
		client.setMessageCallback(callback, counter);

		System.out.println("Successfully set message callback.");

		// Set your token expiry time limit here
		long time = 2400;
		client.setOption("SetSASTokenExpiryTime", time);

		client.registerConnectionStatusChangeCallback(new IotHubConnectionStatusChangeCallbackLogger(), new Object());

		client.open();

		System.out.println("Opened connection to IoT Hub.");

		System.out.println("Beginning to receive messages...");

	}

	/** Used as a counter in the message callback. */
	protected static class Counter {
		protected int num;

		public Counter(int num) {
			this.num = num;
		}

		public int get() {
			return this.num;
		}

		public void increment() {
			this.num++;
		}

		@Override
		public String toString() {
			return Integer.toString(this.num);
		}
	}

	protected static class MessageCallback implements com.microsoft.azure.sdk.iot.device.MessageCallback {
		public IotHubMessageResult execute(Message msg, Object context) {
			Counter counter = (Counter) context;
			System.out.println("Received message " + counter.toString() + " with content: "
					+ new String(msg.getBytes(), Message.DEFAULT_IOTHUB_MESSAGE_CHARSET));
			for (MessageProperty messageProperty : msg.getProperties()) {
				System.out.println(messageProperty.getName() + " : " + messageProperty.getValue());
			}

			int switchVal = counter.get() % 3;
			IotHubMessageResult res;
			switch (switchVal) {
			case 0:
				res = IotHubMessageResult.COMPLETE;
				break;
			case 1:
				res = IotHubMessageResult.ABANDON;
				break;
			case 2:
				res = IotHubMessageResult.REJECT;
				break;
			default:
				// should never happen.
				throw new IllegalStateException("Invalid message result specified.");
			}

			System.out.println("Responding to message " + counter.toString() + " with " + res.name());

			counter.increment();

			return res;
		}
	}

	// Our MQTT doesn't support abandon/reject, so we will only display the messaged
	// received
	// from IoTHub and return COMPLETE
	protected static class MessageCallbackMqtt implements com.microsoft.azure.sdk.iot.device.MessageCallback {
		public IotHubMessageResult execute(Message msg, Object context) {
			Counter counter = (Counter) context;
			System.out.println("Received message " + counter.toString() + " with content: "
					+ new String(msg.getBytes(), Message.DEFAULT_IOTHUB_MESSAGE_CHARSET));
			for (MessageProperty messageProperty : msg.getProperties()) {
				System.out.println(messageProperty.getName() + " : " + messageProperty.getValue());
			}

			counter.increment();

			return IotHubMessageResult.COMPLETE;
		}
	}

	protected static class IotHubConnectionStatusChangeCallbackLogger implements IotHubConnectionStatusChangeCallback {
		@Override
		public void execute(IotHubConnectionStatus status, IotHubConnectionStatusChangeReason statusChangeReason,
				Throwable throwable, Object callbackContext) {
			System.out.println();
			System.out.println("CONNECTION STATUS UPDATE: " + status);
			System.out.println("CONNECTION STATUS REASON: " + statusChangeReason);
			System.out.println("CONNECTION STATUS THROWABLE: " + (throwable == null ? "null" : throwable.getMessage()));
			System.out.println();

			if (throwable != null) {
				throwable.printStackTrace();
			}

			if (status == IotHubConnectionStatus.DISCONNECTED) {
				System.out.println("The connection was lost, and is not being re-established."
						+ " Look at provided exception for how to resolve this issue."
						+ " Cannot send messages until this issue is resolved, and you manually re-open the device client");
			} else if (status == IotHubConnectionStatus.DISCONNECTED_RETRYING) {
				System.out.println("The connection was lost, but is being re-established."
						+ " Can still send messages, but they won't be sent until the connection is re-established");
			} else if (status == IotHubConnectionStatus.CONNECTED) {
				System.out.println("The connection was successfully established. Can send messages.");
			}
		}
	}
}
