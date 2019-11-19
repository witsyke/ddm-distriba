package de.hpi.ddm.actors;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import com.sun.org.apache.xpath.internal.operations.Bool;
import de.hpi.ddm.structures.Password;
import de.hpi.ddm.structures.Task;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import sun.awt.image.ImageWatched;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "master";

	private HashMap<String, String> hintValueStore = new HashMap<>();
	private HashMap<String, String> pwValueStore = new HashMap<>();
	private ArrayList<String []> pwHashMapping = new ArrayList<>();
	private LinkedList<Password> passwords = new LinkedList<Password>();
	private LinkedList<Task> tasks =new LinkedList<Task>();
	private String charset = "";
	private int pwLength = 0;
	private boolean tasksCreationStarted;
	private Boolean inputReadingComplete;
	private Boolean passwordsAllCracked;
	private Boolean hintsAllCracked;

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.inputReadingComplete = false;
		this.passwordsAllCracked = false;
		this.tasksCreationStarted = false;
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
	@NoArgsConstructor
	@AllArgsConstructor
    public static class PasswordCrackRequest implements Serializable {
        private static final long serialVersionUID = 3269154332017915190L;
        private String charSet;
        private String password;
        private int passwordLength;
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
    public static class HintResultBroadcast implements Serializable {            //TODO: every now and then publish all results
        private static final long serialVersionUID = 826765939626353866L;
        private HashMap<String, String> hints;

    }

	@Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HintCrackRequest implements Serializable {
        private static final long serialVersionUID = 2359255535989681327L;
        private HashMap<String, String> hints; //First "Hint" is the chacter the permutation should leave out
        private String characterSet;
        private int start;
        private int end;
        private String missingChar;
    }

	@Data
	public static class HintCrackedAccResult implements  Serializable{
		private static final long serialVersionUID = 4014203645870074601L;
        private HashMap<String, String> hintResultMap;
	}

	@Data
	public static class PasswordCrackedResult implements  Serializable{
		private static final long serialVersionUID  = 1490049316217557786L;
        private String hash;
        private String password;
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
		return receiveBuilder().match(StartMessage.class, this::handle)
                .match(BatchMessage.class, this::handle)
                .match(PasswordCrackedResult.class, this::handle)
                .match(HintCrackedAccResult.class, this::handle)
				.match(Terminated.class, this::handle).match(RegistrationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString())).build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();

		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void handle(BatchMessage message) {

		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.inputReadingComplete = true;
			//Tell all the workers the state of the art //TODO:
			//this.terminate(); //TODO: here this.reader.terminate()?

			//Tell all the workers a task to start with.
			for (ActorRef worker : workers ){
				Task tempTask = tasks.pop();
				worker.tell(new HintCrackRequest( this.hintValueStore, tempTask.characterSet, tempTask.start,  tempTask.end,   tempTask.missingChar), this.self());
			}

			return;
		}

		for (String[] line : message.getLines()){
			if(tasksCreationStarted == false){
				this.charset = line[2];
				this.pwLength = Integer.parseInt(line[3]);
				this.tasksCreationStarted = true;
				generateTasks(); //TODO: has to be parallelized
			}

			System.out.println(Arrays.toString(line));

			passwords.add(new Password(line[4], Arrays.asList(Arrays.copyOfRange(line, 5, line.length)))); //Add all pws with hints to a list --> TODO extend/ use to/ as queue
			//pwHashMapping.add((String[])Arrays.copyOfRange(line, 5, line.length)); //Add all hints for a password in ArrayList
			pwValueStore.put(line[4], null);

			for(int i = 5; i < line.length; i++){ //The hints start at position 5
				hintValueStore.put(line[i], null);
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

		//If a new worker joins, give him a task.
        if(this.inputReadingComplete == true & this.hintsAllCracked == false){
        	Task tempTask = tasks.pop();
        	this.sender().tell(new HintCrackRequest( this.hintValueStore, tempTask.characterSet, tempTask.start,  tempTask.end,   tempTask.missingChar), this.sender());
        } else if (this.inputReadingComplete == true & this.hintsAllCracked == true){
        	Password tempPw = passwords.pop();
        	this.sender().tell(new PasswordCrackRequest(this.charset, tempPw.password, this.pwLength), this.self());
		}
	}

	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
	}

	protected void handle(HintCrackedAccResult message){
        hintValueStore.putAll(message.hintResultMap);
		if(hintValueStore.containsValue(null)){
			this.log().info("All hints cracked");
			hintsAllCracked = true;
			//TODO: send all hint abort messages to the workers & start spreading the PWS
			//TODO: map all hints to the PWs and send them around
		}

    }

    protected void handle(PasswordCrackedResult message){
		pwValueStore.put(message.getHash(), message.getPassword()); // why are we doing this? think this could be changed to be the queue with the pw tasks and here
		//marked as complete and forwarded to collector
		this.collector.tell(message.getPassword(), this.self());
		if(!pwValueStore.containsValue(null)){
			this.log().info("All passwords cracked!");
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
		}
	}

	protected void generateTasks(){
		//TODO
	}


}
