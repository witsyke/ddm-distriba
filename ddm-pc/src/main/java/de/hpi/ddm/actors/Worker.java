package de.hpi.ddm.actors;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
        private static final long serialVersionUID = 556144565218770270L;
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

	private void handle(Master.PasswordCrackRequest message){
		//TODO
		//Password hash (before all the hints were updated)
		//

	}

    private void handle(Master.HintCrackRequest message) {

        if (!this.characterSet.equals(message.getCharacterSet())) {
            this.characterSet = message.getCharacterSet();
        }
        if (this.characterSet.length() != message.getCharacterSet().length()) {
            this.generateFactorials(this.characterSet);
        }

        this.calculatePermutationsAndCheckIfHint(this.characterSet, message.getStart(), message.getEnd(), message.getMissingChar());

        this.sender().tell(new CompletedRangeMessage(this.hints), this.self()); // this can act as gimme work message at the same time

    }

	private void handle(Master.HintCrackAbort message){
		//TODO
		//Abort function
	}

    private void handle(Master.PasswordCrackAbort message) {
        //TODO:
        //Abort function
        // SW: don't think we actually need this
    }

	private void handle(Master.HintResultBroadcast message){
		//TODO:
	}


	private String crack_password(String hash, String hints){
		// Crack password
		return null;
	}

	private String crack_hint(String hash, String hints){
		//Crack hint
		return null;
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
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}


    private void calculatePermutationsAndCheckIfHint(String string, int start, int end, String character) {
        // final end = factorials[string.length()] = length!
        for (int i = start; i < end; i++) {
            StringBuilder onePermutation = new StringBuilder();
            String temp = string;
            int positionCode = i;
            for (int position = string.length(); position > 0; position--) {
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

    private void calculatePermutationsAndCheckIfPassword(char[] set, String prefix, int n, int k, String password) {

        if (this.passwordFound) {
            return;
        }

        if (k == 0) {
            if (this.hash(prefix).equals(password)) {
                this.passwordFound = true;
                this.cleanPassword = this.hash(prefix);
            }
            return;
        }

        for (int i = 0; i < n; ++i) {
            String newPrefix = prefix + set[i];
            calculatePermutationsAndCheckIfPassword(set, newPrefix, n, k - 1, password);
        }
    }



	/* Hints:
	 * Both Passwords & hints are SHA-256 encrypted
	 * Encryption cracking via burte force approach
	 * generate sequences
	 * compare current SHA-256 with exising one -> if the same -> encryption is broken
	 */

	/* Rules:
	 * New shutdown protocol
	 * propper communication protocol for workers/masters
	 * additional actors
	 */

	// Password Cracking:
		/*
			Parameters that changes:
				* password length
				* password chars
				* number of hints ("width of file")
				* number of passwords (lenth of the file)
				* number of cluster nodes (do not wait for x nodes to join the cluster; you do not know their number, Implement elasticity, i.e. allow joining nodes at runtime)

			Parameters that do not change
				* encryption function SHA256
				* all passwords are of the same length and have the same character universe
		*/

	
}
