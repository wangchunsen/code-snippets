package csw.snnips

import java.util.concurrent.locks.{Condition, ReentrantLock}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object KeyLock {

  private class Entry(val thread: Thread, var lockCount: Int = 1, var condition: Option[Condition] = None)

}
  class KeyLock[K] {

    import KeyLock._

    private val table: mutable.HashMap[K, Entry] = new mutable.HashMap()
    private val lock: ReentrantLock = new ReentrantLock()

    private def lockOnT[T](key: K, time: Long)(task: => T): Option[Try[T]] =
      Option.when(acquireLock(key, time)) {
        val res = Try(task)
        releaseLock(key)
        res
      }

    private def lockOnF[T](key: K, time: Long)(future: => Future[T]): Option[Future[T]] =
      Option.when(acquireLock(key, time)) {
        val f = future
        f.onComplete(_ => releaseLock(key))(ExecutionContext.global)
        f
      }


    def tryWithLock[T](keyLock: K)(task: => T): Option[Try[T]] = lockOnT(keyLock, 0)(task)

    def tryWithLockF[T](keyLock: K)(f: => Future[T]): Option[Future[T]] = lockOnF(keyLock, 0)(f)

    def withLock[T](keyLock: K, timeoutNanos: Long)(task: => T): Try[T] =
      lockOnT(keyLock, timeoutNanos)(task).getOrElse(throw new Exception("time out"))

    def withLockF[T](keyLock: K, timeoutNanos: Long)(f: => Future[T]): Future[T] =
      lockOnF(keyLock, timeoutNanos)(f).getOrElse(throw new Exception("time out"))


    private def acquireLock(key: K, timeoutNanos: Long): Boolean = {
      @tailrec def tailRecAcquire(remainingNano: Long): Boolean = {
        table.get(key) match {
          case Some(entry) if entry.thread eq Thread.currentThread() =>
            entry.lockCount += 1
            true
          case Some(entry) =>
            if (remainingNano <= 0) false
            else {
              val condition = getOrCreatCondition(entry)
              val remaining = condition.awaitNanos(remainingNano)
              tailRecAcquire(remaining)
            }
          case None =>
            table.put(key, new Entry(Thread.currentThread()))
            true
        }
      }

      lock.lock()
      try tailRecAcquire(timeoutNanos)
      finally lock.unlock()
    }

    private def releaseLock(key: K): Unit = {
      lock.lock()
      try {
        val entry = table(key)
        require(entry.thread eq Thread.currentThread())
        if (entry.lockCount == 1) {
          table.remove(key)
        } else {
          entry.lockCount -= 1
        }
        entry.condition.foreach(_.signalAll())
      } finally lock.unlock()
    }

    private def getOrCreatCondition(entry: Entry): Condition = entry.condition match {
      case Some(c) => c
      case None =>
        val newCondition = lock.newCondition()
        entry.condition = Some(newCondition)
        newCondition
    }
  }