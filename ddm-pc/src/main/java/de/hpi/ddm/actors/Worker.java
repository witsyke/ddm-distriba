package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Worker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";

    public static Props props() {
        return Props.create(Worker.class);
    }

    public Worker() {
        this.cluster = Cluster.get(this.context().system());
    }

    ////////////////////
    // Actor Messages //
    ////////////////////


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CompletedRangeMessage implements Serializable {
        private static final long serialVersionUID = -7216189678857844911L;
        private HashMap<String, String> hints;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FoundPasswordMessage implements Serializable {
        private static final long serialVersionUID = -4588265321773490936L;
        private String hash;
        private String password;
    }


    /////////////////
    // Actor State //
    /////////////////

    private Member masterSystem;
    private final Cluster cluster;
    private HashMap<String, String> hints;
    private int[] factorials;
    private String characterSet = "";
    private boolean passwordFound;
    private String cleanPassword;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);

        this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(this.self());
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CurrentClusterState.class, this::handle)
                .match(MemberUp.class, this::handle)
                .match(MemberRemoved.class, this::handle)
                .match(Master.HintCrackRequest.class, this::handle)
                .match(Master.HintCrackAbort.class, this::handle)
                .match(Master.PasswordCrackRequest.class, this::handle)
                .match(Master.HintResultBroadcast.class, this::handle)
                .match(Master.PasswordCrackAbort.class, this::handle)
                .match(Master.PasswordInitCrackRequest.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(CurrentClusterState message) {
        message.getMembers().forEach(member -> {
            if (member.status().equals(MemberStatus.up()))
                this.register(member);
        });
    }

    private void handle(MemberUp message) {
        this.register(message.member());

    }

    private void register(Member member) {
        if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
            this.masterSystem = member;

            this.getContext()
                    .actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
                    .tell(new Master.RegistrationMessage(), this.self());
        }
    }

    private void handle(MemberRemoved message) {
        if (this.masterSystem.equals(message.member()))
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private void handle(Master.PasswordCrackRequest message) {
        this.passwordFound = false; // have to reset this every time because i was too stupid to get the correction right
        this.cleanPassword = "";
        String charSet = message.getCharSet();

        for (String hint : message.getPassword().getHints()) {
            charSet = charSet.replace(this.hints.get(hint), ""); // this assumes that all hints are actually there, if this fails consider checking presence of hint before usage
        }

        char[] chars = charSet.toCharArray();
        String hashedPassword = message.getPassword().getPassword();

        this.calculatePermutationsAndCheckIfPassword(chars, "", chars.length, message.getPasswordLength(), hashedPassword);

        this.sender().tell(new FoundPasswordMessage(hashedPassword, this.cleanPassword), this.self()); // this can act as gimme work message at the same time
    }

    private void handle(Master.PasswordInitCrackRequest message) {
        this.hints = message.getHints();
        this.passwordFound = false; // have to reset this every time because i was too stupid to get the correction right
        this.cleanPassword = "";
        String charSet = message.getCharSet();

        for (String hint : message.getPassword().getHints()) {
            charSet = charSet.replace(this.hints.get(hint), ""); // this assumes that all hints are actually there, if this fails consider checking presence of hint before usage
        }

        char[] chars = charSet.toCharArray();
        String hashedPassword = message.getPassword().getPassword();

        this.calculatePermutationsAndCheckIfPassword(chars, "", chars.length, message.getPasswordLength(), hashedPassword);

        this.sender().tell(new FoundPasswordMessage(hashedPassword, this.cleanPassword), this.self()); // this can act as gimme work message at the same time
    }


    private void handle(Master.HintCrackRequest message) {
        this.hints = message.getHints();

        if (this.characterSet.length() != message.getCharacterSet().length()) {
            System.out.println("generating factorials ...");
            this.generateFactorials(message.getCharacterSet());
        }
        this.characterSet = message.getCharacterSet();

        this.calculatePermutationsAndCheckIfHint(this.characterSet, message.getStart(), message.getEnd(), message.getMissingChar());

        this.sender().tell(new CompletedRangeMessage(this.hints), this.self()); // this can act as gimme work message at the same time

    }

    private void handle(Master.HintCrackAbort message) {
        //TODO
        //Abort function
    }

    private void handle(Master.PasswordCrackAbort message) {
        //TODO:
        //Abort function
        // SW: don't think we actually need this
    }

    private void handle(Master.HintResultBroadcast message) {
        //TODO:
    }

    private String hash(String line) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));

            StringBuffer stringBuffer = new StringBuffer();
            for (int i = 0; i < hashedBytes.length; i++) {
                stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }


    private void calculatePermutationsAndCheckIfHint(String charSet, int start, int end, String character) {

        // final end = factorials[string.length()] = length!
        for (int i = start; i < end; i++) {
            StringBuilder onePermutation = new StringBuilder();
            String temp = charSet;
            int positionCode = i;
            for (int position = charSet.length(); position > 0; position--) {
                int selected = positionCode / this.factorials[position - 1];
                onePermutation.append(temp.charAt(selected));
                positionCode = positionCode % this.factorials[position - 1];
                temp = temp.substring(0, selected) + temp.substring(selected + 1);
            }
            String hashedPermutation = hash(onePermutation.toString());
            if (this.hints.containsKey(hashedPermutation)) {
                this.hints.put(hashedPermutation, character);
            }
        }
    }

    private void generateFactorials(String string) {
        factorials = new int[string.length() + 1];
        factorials[0] = 1;
        for (int i = 1; i <= string.length(); i++) {
            factorials[i] = factorials[i - 1] * i;
        }
    }

    private void calculatePermutationsAndCheckIfPassword(char[] set, String word, int n, int k, String password) {

        if (this.passwordFound) {
            return;
        }
        if (k == 0) {
            if (this.hash(word).equals(password)) {
                this.cleanPassword = word;
                this.passwordFound = true;
            }
            return;
        }

        for (int i = 0; i < n; ++i) {
            String newPrefix = word + set[i];
            calculatePermutationsAndCheckIfPassword(set, newPrefix, n, k - 1, password);
        }
    }
}
