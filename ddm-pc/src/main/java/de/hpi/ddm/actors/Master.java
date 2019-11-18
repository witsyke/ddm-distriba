package de.hpi.ddm.actors;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.HashMap;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "master";

	private HashMap<String, String> hintValueStore = new HashMap<>();
	private HashMap<String, String> pwValueStore = new HashMap<>();
	private ArrayList<String []> pwHashMapping = new ArrayList<>();

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = 388281792843702459L;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	// New actor messages

	@Data
	public static class PasswordCrackRequest implements Serializable {
		private static final long serialVersionUID = 3269154332017915190L;
	}

	@Data
	public static class PasswordCrackAbort implements Serializable {
		private static final long serialVersionUID = 8134160671290471710L;
	}

	@Data
	public static class HintCrackAbort implements Serializable{
		private static final long serialVersionUID = 8428904686127096286L;
	}

	@Data
	public static class HintCrackRequest implements Serializable{
		private static final long serialVersionUID = 2359255535989681327L;
	}

	@Data
	public static class HintCrackedAccResult implements  Serializable{
		private static final long serialVersionUID = 4014203645870074601L;
	}

	@Data
	public static class PasswordCrackedResult implements  Serializable{
		private static final long serialVersionUID  = 1490049316217557786L;
	}


	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;

	private long startTime;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(StartMessage.class, this::handle).match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle).match(RegistrationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString())).build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();

		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void handle(BatchMessage message) {

		//TODO: For each PW we need -> ID, PasswordHash, Hints


		//TODO: create key value store of the password-hashes and the values & a respecitve key-value store for the hints


		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			//this.terminate(); //TODO: here this.reader.terminate()?
			return;
		}

		for (String[] line : message.getLines()){
			System.out.println(Arrays.toString(line));


			pwHashMapping.add((String[])Arrays.copyOfRange(line, 5, line.length)); //Add all hints for a password in ArrayList
			pwValueStore.put(line[3], "");
			for(int i = 5; i < line.length; i++){ //The hints start at position 5
				hintValueStore.put(line[i], "");
			}
		}

		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()),
				this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());

		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}

		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());

		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
		this.log().info("Registered {}", this.sender());
	}

	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
	}
}
