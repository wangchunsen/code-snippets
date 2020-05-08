import java.util
import java.util.concurrent.ConcurrentLinkedQueue

import csw.snnips.{KeyLock, Var}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext
import scala.util.Success


class Main2Test extends AnyFlatSpec with Matchers {
  private def thread(run: => Any) {
    val tt = new Thread(() => run)
    tt.start()
  }

  it should "run " in {
    val keylock = new KeyLock[String]()

    def tt(key: String, job: String): Unit = {
      val tt = new Thread(() => {
        keylock.withLock(key, Long.MaxValue) {
          println(s"executing job $job on key $key")
          Thread.sleep(2000)
        }
      })
      tt.start()

    }

    0 to 12 foreach { a =>
      tt("key1", s"job $a")
      tt("key2", s"job $a")
    }

    Thread.sleep(7000)
  }

  it should "run again " in {
    val keylock = new KeyLock[String]()
    keylock.withLock("aaaa", Long.MaxValue) {
      println("aaa")
    }
  }

  it should "throw timeout exception" in {
    val keyLock = new KeyLock[String]()
    thread {
      keyLock.tryWithLock("key1") {
        Thread.sleep(2000)
      }
    }
    keyLock.withLock("key1", 500) {

    }
  }

  "a thread" should "able to re-entrant a lock" in {
    val keylock = new KeyLock[String]()
    keylock.withLock("aaaa", Long.MaxValue) {
      keylock.withLock("aaaa", Long.MaxValue) {
        println("lalalallalal")
      }
    }
  }

  "a act" should "works" in {
    val list = new ConcurrentLinkedQueue[Int]()
    val act = new Var[Int](0)
    0 until 100 foreach { _ =>
      thread {
        act.execute(_ + 1).andThen {
          case Success(value) => list.add(value)
        }(ExecutionContext.global)
      }
    }
    Thread.sleep(20)
    new util.HashSet(list) should have size 100
  }
}
