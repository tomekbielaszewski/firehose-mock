package io.timson.firehose.stream;

import io.timson.firehose.request.CreateDeliveryStreamRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class DeliveryStreamService {
    private static final Logger logger = Logger.getLogger(DeliveryStreamService.class.getName());

    private final Map<String, DeliveryStream> deliveryStreams = new HashMap<>();
    private final DeliveryStreamFactory deliveryStreamFactory;

    public DeliveryStreamService(DeliveryStreamFactory deliveryStreamFactory) {
        this.deliveryStreamFactory = deliveryStreamFactory;
    }

    public void write(String deliveryStream, String data) {
        logger.info("Writing data to delivery stream with name " + deliveryStream);
        DeliveryStream stream = getDeliveryStream(deliveryStream);
        stream.write(data);
    }

    public void createStream(CreateDeliveryStreamRequest createStreamRequest) {
        final String name = createStreamRequest.getName();
        logger.info("Creating delivery stream with name " + name);

        if (deliveryStreams.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Stream with name %s already exists", name));
        }
        DeliveryStream deliveryStream = deliveryStreamFactory.fromRequest(createStreamRequest);
        deliveryStreams.put(name, deliveryStream);

        logger.info("Delivery stream " + name + " successfully created");
    }

    public void deleteStream(String name) {
        logger.info("Deleting delivery stream with name " + name);

        if (!deliveryStreams.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Stream with name %s does not exists", name));
        }
        DeliveryStream deliveryStream = getDeliveryStream(name);
        deliveryStream.stop();
        deliveryStreams.remove(name);

        logger.info("Delivery stream " + name + " successfully deleted");
    }

    public Set<String> listStreams() {
        logger.info("Getting all delivery stream names");
        return deliveryStreams.keySet();
    }

    private DeliveryStream getDeliveryStream(String name) {
        logger.info("Getting delivery stream by name: " + name);
        DeliveryStream stream = deliveryStreams.get(name);
        if (stream == null) {
            throw new IllegalArgumentException(String.format("Unknown delivery stream %s", name));
        }
        return stream;
    }

}
