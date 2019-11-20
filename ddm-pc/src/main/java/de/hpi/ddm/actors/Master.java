package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;

import de.hpi.ddm.structures.Password;
import de.hpi.ddm.structures.Task;
import de.hpi.ddm.structures.Work;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";
    private static final int BASE_RANGE_SIZE = 750000;

    public static Props props(final ActorRef reader, final ActorRef collector) {
        return Props.create(Master.class, () -> new Master(reader, collector));
    }

    public Master(final ActorRef reader, final ActorRef collector) {
        this.reader = reader;
        this.collector = collector;
        this.workers = new ArrayList<>();
    }

    ////////////////////
    // Master Messages //
    ////////////////////

    @Data
    public static class StartMessage implements Serializable {
        private static final long serialVersionUID = 388281792843702459L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data
    static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class PasswordInitCrackRequest implements Serializable {
        private static final long serialVersionUID = 7147056297234151997L;
        private HashMap<String, String> hints;
        private String charSet;
        private Password password;
        private int passwordLength;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class PasswordCrackRequest implements Serializable {
        private static final long serialVersionUID = 3269154332017915190L;
        private String charSet;
        private Password password;
        private int passwordLength;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class HintCrackRequest implements Serializable {
        private static final long serialVersionUID = 2359255535989681327L;
        private String characterSet;
        private int start;
        private int end;
        private String missingChar;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class HintInitCrackRequest implements Serializable {
        private static final long serialVersionUID = 5508620954947621835L;
        private HashMap<String, String> hints;
        private String characterSet;
        private int start;
        private int end;
        private String missingChar;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef reader;
    private final ActorRef collector;
    private final List<ActorRef> workers;

    private long startTime;

    private HashMap<String, String> hintSolutionStore = new HashMap<>();
    private HashMap<String, String> passwordSolutionStore = new HashMap<>();
    private LinkedList<Password> passwordTasks = new LinkedList<>();
    private LinkedList<Task> hintRangeTasks = new LinkedList<>();
    private HashMap<ActorRef, Work> work = new HashMap<>();
    private String charSet = "";
    private int passwordLength = 0;
    private boolean inputReadingComplete;
    private boolean allHintsCracked;

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
        return receiveBuilder()
                .match(StartMessage.class, this::handle)
                .match(BatchMessage.class, this::handle)
                .match(Worker.FoundPasswordMessage.class, this::handle)
                .match(Worker.CompletedRangeMessage.class, this::handle)
                .match(Terminated.class, this::handle).match(RegistrationMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString())).build();
    }

    private void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();
        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    private void handle(BatchMessage message) {

        if (message.getLines().isEmpty()) {
            this.collector.tell(new Collector.PrintMessage(), this.self());
            this.inputReadingComplete = true;
            startUpWorkers(); // give all known workers their first assignment
            return;
        }
        // get one line to extract charSet and password length to use for task generation
        String[] line1 = message.getLines().get(1);
        this.charSet = line1[2];
        this.passwordLength = Integer.parseInt(line1[3]); // could do this later

        generateHintTasks();
        // Collections.shuffle(this.hintRangeTasks); // --> might be faster for some inputs, but not for the provided example

        for (String[] line : message.getLines()) {
            passwordTasks.add(new Password(line[4], Arrays.copyOfRange(line, 5, line.length)));
            passwordSolutionStore.put(line[4], null);

            for (int i = 5; i < line.length; i++) { //The hints start at position 5
                hintSolutionStore.put(line[i], null);
            }
        }

        this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    private void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.workers.add(this.sender());
        this.log().info("Registered {}", this.sender());

        //If a new worker joins, give him a task.
        if (this.inputReadingComplete && !this.allHintsCracked) {
            this.log().debug("Handing first hint task on registration to " + this.sender());
            Task tempTask = hintRangeTasks.pop();
            this.work.put(this.sender(), tempTask);
            this.sender().tell(new HintInitCrackRequest(this.hintSolutionStore, tempTask.characterSet, tempTask.start, tempTask.end, tempTask.missingChar), this.self());
        } else if (this.inputReadingComplete) {
            this.log().debug("Handing first password task on registration to " + this.sender());
            Password tempPw = passwordTasks.pop();
            this.work.put(this.sender(), tempPw);
            this.sender().tell(new PasswordInitCrackRequest(this.hintSolutionStore, this.charSet, tempPw, this.passwordLength), this.self());
        }
    }

    private void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
        this.log().info("Unregistered {}", message.getActor());
    }

    private void handle(Worker.CompletedRangeMessage message) {
        this.log().debug("Merging hints from " + this.sender());

        hintSolutionStore.putAll(message.getHints());
        if (!hintSolutionStore.containsValue(null) && !passwordTasks.isEmpty()) {
            this.log().info("All hints cracked");
            allHintsCracked = true;
            Password tempPw = passwordTasks.pop();
            this.work.put(this.sender(), tempPw);
            this.sender().tell(new PasswordInitCrackRequest(this.hintSolutionStore, this.charSet, tempPw, this.passwordLength), this.self());
            this.work.forEach((k, v) -> {
                if (v == null && !passwordTasks.isEmpty()) {
                    Password pop = passwordTasks.pop();
                    this.work.put(k, pop);
                    k.tell(new PasswordInitCrackRequest(this.hintSolutionStore, this.charSet, pop, this.passwordLength), this.self());
                }
            });
        } else if (!hintRangeTasks.isEmpty()) {
            this.log().debug("Giving worker new hint task. Remaining hint ranges: " + hintRangeTasks.size());
            Task tempTask = hintRangeTasks.pop();
            this.work.put(this.sender(), tempTask);
            this.sender().tell(new HintCrackRequest(tempTask.characterSet, tempTask.start, tempTask.end, tempTask.missingChar), this.self());
        } else {
            this.log().debug("No more hints available, going to idle");
            this.work.put(this.sender(), null);
        }

    }

    private void handle(Worker.FoundPasswordMessage message) {
        this.passwordSolutionStore.put(message.getHash(), message.getPassword());
        this.collector.tell(new Collector.CollectMessage(message.getPassword()), this.self());
        if (!passwordSolutionStore.containsValue(null)) {
            this.log().debug("All passwords cracked!");
            this.collector.tell(new Collector.PrintMessage(), this.self());
            this.terminate();
        } else if (!passwordTasks.isEmpty()) {
            Password tempPw = passwordTasks.pop();
            this.work.put(this.sender(), tempPw);
            this.sender().tell(new PasswordCrackRequest(this.charSet, tempPw, this.passwordLength), this.self());
        } else {
            this.work.put(this.sender(), null);
        }

    }

    private void startUpWorkers() {
        for (ActorRef worker : workers) {
            if (!hintRangeTasks.isEmpty()) {
                Task tempTask = hintRangeTasks.pop();
                worker.tell(new HintInitCrackRequest(this.hintSolutionStore, tempTask.characterSet, tempTask.start, tempTask.end, tempTask.missingChar), this.self());
            }
        }

    }

    private void generateHintTasks() {
        int permutationsPerHint = factorial(charSet.length() - 1);

        for (char c : this.charSet.toCharArray()) {
            String missingChar = String.valueOf(c);
            String tempCharSet = this.charSet.replace(missingChar, "");
            for (int i = 0; i < permutationsPerHint; i += BASE_RANGE_SIZE) {
                if (i + BASE_RANGE_SIZE >= permutationsPerHint) {
                    hintRangeTasks.push(new Task(tempCharSet, i, permutationsPerHint, missingChar));
                } else {
                    hintRangeTasks.push(new Task(tempCharSet, i, i + BASE_RANGE_SIZE, missingChar));
                }
            }
        }
    }

    private void terminate() {
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

    private int factorial(int n) {
        if (n <= 2) {
            return n;
        }
        return n * factorial(n - 1);
    }
}
