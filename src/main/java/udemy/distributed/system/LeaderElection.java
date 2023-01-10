package udemy.distributed.system;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class LeaderElection implements Watcher {
	
	private static final String ZOOKEEPER_ADDR = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;
	private static final String ELECTION_NAMESPACE = "/election";
	
	private ZooKeeper zooKeeper;
	private String currentZnodeName;
	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		LeaderElection leaderElection = new LeaderElection();
		
		leaderElection.connectToZookeeper();
		leaderElection.volunteerForLeadership();
		leaderElection.electLeader();
		leaderElection.run();
		leaderElection.close();
		System.out.println("disconnected from zookeeper, exiting app");
	}
	
	public void volunteerForLeadership() throws KeeperException, InterruptedException {
		String znodePrefix = ELECTION_NAMESPACE + "/c";
		String znodeFullPath = zooKeeper.create(znodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		System.out.println("znode name = " + znodeFullPath);
		this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
	}
	
	public void electLeader() throws KeeperException, InterruptedException {
		List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
		
		Collections.sort(children);
		String smallestChild = children.get(0);
		
		if (smallestChild.equals(currentZnodeName)) {
			System.out.println("I am the leader");
			return;
		}
		System.out.println("I am not the leader, " + smallestChild);
	}
	
	public void connectToZookeeper() throws IOException {
		zooKeeper = new ZooKeeper(ZOOKEEPER_ADDR, SESSION_TIMEOUT, this);
	}
	
	public void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}
	}
	
	public void close() throws InterruptedException {
		zooKeeper.close();
	}

	@Override
	public void process(WatchedEvent event) {
		switch (event.getType()) {
		case None:
			if (event.getState() == Event.KeeperState.SyncConnected) {
				System.out.println("successfully connected to Zookeeper");
			} else {
				synchronized (zooKeeper) {
					System.out.println("disconnected from Zookeeper event");
					zooKeeper.notifyAll();
				}
			}
		}
		
	}
}
