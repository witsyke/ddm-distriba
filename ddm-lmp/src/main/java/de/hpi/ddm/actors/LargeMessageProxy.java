package de.hpi.ddm.actors;

import java.io.*;

import akka.actor.*;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import akka.stream.StreamRefResolver;
import akka.stream.impl.streamref.StreamRefResolverImpl;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import de.hpi.ddm.structures.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {
    ////////////////////////
    // Actor Construction //
    ////////////////////////
    public static final String DEFAULT_NAME = "largeMessageProxy";

    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessage<T> implements Serializable {
        private static final long serialVersionUID = 5257188484780972570L;
        private T message;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SourceRefMessage implements Serializable {
        private static final long serialVersionUID = -5370521216104792175L;
        private String sourceRef;
        private ActorRef sender;
        private ActorRef receiver;
    }

    /////////////////
    // Actor State //
    /////////////////
    private Materializer materializer = Materializer.createMaterializer(this.context()); // test matFromSystem performance
    private StreamRefResolver streamRefResolver = new StreamRefResolverImpl((ExtendedActorSystem) this.context().system());

    /////////////////////
    // Actor Lifecycle //
    /////////////////////
    ////////////////////
    // Actor Behavior //
    ////////////////////
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LargeMessage.class, this::handle)
                .match(SourceRefMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(LargeMessage<?> message) {
        ActorRef receiver = message.getReceiver();
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

        ByteArrayInputStream test = new ByteArrayInputStream(KryoPoolSingleton.get().toBytesWithClass(message.getMessage()));

        SourceRef<ByteString> sourceRef = StreamConverters.fromInputStream(() ->
                test, 65536) // chunkSize is 2^16 -> found to be good chunk size by trail
                .runWith(StreamRefs.sourceRef(), materializer);
        receiverProxy.tell(new SourceRefMessage(streamRefResolver.toSerializationFormat(sourceRef), this.sender(), message.getReceiver()), this.self());
        test.reset();
    }

    private void handle(SourceRefMessage message) {
        SourceRef<ByteString> objectSourceRef = streamRefResolver.resolveSourceRef(message.sourceRef);

        ByteStringBuilder builder = ByteString.createBuilder();
        objectSourceRef.getSource().runWith(StreamConverters.asJavaStream(), materializer)
                .forEach(builder::append);

        message.getReceiver().tell(KryoPoolSingleton.get().fromBytes(builder.result().toArray()), message.getSender());

        builder.clear();
    }
}