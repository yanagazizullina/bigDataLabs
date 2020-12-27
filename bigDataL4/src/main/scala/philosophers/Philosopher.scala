package philosophers

import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

import java.util.concurrent.Semaphore
import scala.util.Random

case class Philosopher(id: Int,
                       hostPort: String,
                       root: String,
                       left: Semaphore,
                       right: Semaphore,
                       seats: Integer)extends Watcher {

  val zk = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()
  val philosopherPath: String = root + "/" + id.toString

  if (zk == null) throw new Exception("ZK is NULL.")

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      mutex.notify()
    }
  }

  def eat(): Boolean = {
    printf("The philosopher %d is going to eat\n", id)
    mutex.synchronized{
      var created = false
      while (true){
        if (!created){
          zk.create(philosopherPath, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
          created = true
        }
        val active = zk.getChildren(root, this)
        if (active.size()> seats){
          zk.delete(philosopherPath, -1)
          mutex.wait(3000)
          Thread.sleep(Random.nextInt(5) * 100)
          created = false
        }else{
          left.acquire()
          printf("The philosopher %d took the left fork\n", id)
          right.acquire()
          printf("The philosopher %d took the right fork\n", id)
          Thread.sleep((Random.nextInt(5) + 1) * 1000)
          right.release()
          printf("The philosopher %d put  down the right fork\n", id)
          left.release()
          printf("The philosopher %d put the left fork and stopped eating\n", id)
          return true
        }
      }
    }
    false
  }

  def think(): Unit = {
    printf("The philosopher %d thinks\n", id)
    zk.delete(philosopherPath, -1)
    Thread.sleep((Random.nextInt(5) + 1) * 1000)
  }
}
