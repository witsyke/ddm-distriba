package de.hpi.ddm.actors;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

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
import com.beust.jcommander.internal.Sets;
import de.hpi.ddm.MasterSystem;
import de.hpi.ddm.structures.Password;
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
    static class CompletedRangeMessage implements Serializable {
        private static final long serialVersionUID = -7216189678857844911L;
        private HashMap<String, String> hints;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class FoundPasswordMessage implements Serializable {
        private static final long serialVersionUID = -4588265321773490936L;
        private String hash;
        private String password;
    }

    /////////////////
    // Actor State //
    /////////////////

    private Member masterSystem;
    private final Cluster cluster;

    private Set<String> hints = Sets.newHashSet();
    private HashMap<String, String> finalHints;
    private HashMap<String, String> crackedHints = new HashMap<>();
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
                .match(MemberUp.class, this::handle)
                .match(MemberRemoved.class, this::handle)
                .match(CurrentClusterState.class, this::handle)
                .match(Master.HintCrackRequest.class, this::handle)
                .match(Master.PasswordCrackRequest.class, this::handle)
                .match(Master.PasswordInitCrackRequest.class, this::handle)
                .match(Master.HintInitCrackRequest.class, this::handle)
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

    private void handle(Master.PasswordInitCrackRequest message) {
        this.finalHints = message.getHints();
        crackPassword(message.getCharSet(), message.getPassword(), message.getPasswordLength());
    }

    private void handle(Master.PasswordCrackRequest message) {
        crackPassword(message.getCharSet(), message.getPassword(), message.getPasswordLength());

    }

    private void handle(Master.HintInitCrackRequest message) {
        this.hints.addAll(message.getHints());
        crackHint(message.getCharacterSet(), message.getStart(), message.getEnd(), message.getMissingChar());
    }

    private void handle(Master.HintCrackRequest message) {
        crackHint(message.getCharacterSet(), message.getStart(), message.getEnd(), message.getMissingChar());

    }

    ////////////////////
    // Actor Helpers  //
    ////////////////////

    private String hash(String line) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes(StandardCharsets.UTF_8));

            StringBuilder stringBuffer = new StringBuilder();
            for (byte hashedByte : hashedBytes) {
                stringBuffer.append(Integer.toString((hashedByte & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage()); //NOSONAR
        }
    }

    private void crackHint(String characterSet, int start, int end, String missingChar) {
        if (this.characterSet.length() != characterSet.length()) {
            this.generateFactorials(characterSet);
        }
        this.characterSet = characterSet;

        this.calculatePermutationsAndCheckIfHint(this.characterSet, start, end, missingChar);

        this.sender().tell(new CompletedRangeMessage(this.crackedHints), this.self());
    }

    private void calculatePermutationsAndCheckIfHint(String charSet, int start, int end, String character) {
        this.crackedHints.clear();
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
            if (this.hints.remove(hashedPermutation)) {
                this.crackedHints.put(hashedPermutation, character);

            }
        }
    }

    // different form the basic factorial function in the master, as this saves the factorial for each step in an array
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
                this.cleanPassword = word; // not too happy with these global variables, but not too sure how to change
                this.passwordFound = true;
            }
            return;
        }

        for (int i = 0; i < n; ++i) {
            String newPrefix = word + set[i];
            calculatePermutationsAndCheckIfPassword(set, newPrefix, n, k - 1, password);
        }
    }

    // extracted this to avoid duplication between Init and normal PW crack message
    private void crackPassword(String baseCharSet, Password password, int passwordLength) {
        this.passwordFound = false;
        this.cleanPassword = "";
        String charSet = baseCharSet;

        for (String hint : password.getHints()) {
            charSet = charSet.replace(this.finalHints.get(hint), ""); // this assumes that all hints are actually there, if this fails consider checking presence of hint before usage
        }

        char[] chars = charSet.toCharArray();
        String hashedPassword = password.getPassword();

        this.calculatePermutationsAndCheckIfPassword(chars, "", chars.length, passwordLength, hashedPassword);

        this.sender().tell(new FoundPasswordMessage(hashedPassword, this.cleanPassword), this.self()); // this can act as gimme work message at the same time
    }
}
