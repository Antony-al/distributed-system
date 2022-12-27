package sandbox.distributed.system;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class LeaderElection implements Watcher {
	
	private static final String ZOOKEEPER_ADDR = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;
	
	private ZooKeeper zooKeeper;
	
	public static void main(String[] args) throws IOException {
		LeaderElection leaderElection = new LeaderElection();
		leaderElection.connectToZookeeper();
	}
	
	public void connectToZookeeper() throws IOException {
		zooKeeper = new ZooKeeper(ZOOKEEPER_ADDR, SESSION_TIMEOUT, this);
	}

	@Override
	public void process(WatchedEvent event) {
		switch (event.getType()) {
		case None:
			if (event.getState() == Event.KeeperState.SyncConnected) {
				System.out.println("successfully connected to Zookeeper");
			}
		}
		
	}
}
