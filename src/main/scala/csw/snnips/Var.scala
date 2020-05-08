package csw.snnips

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.{Future, Promise}
private case class State[T](currentRun: Option[T], pending: Queue[T]) {
  def setRunning(running: T): State[T] = this.copy(currentRun = Some(running))

  def setIdeal(): State[T] = this.copy(currentRun = None)

  def enqueue(task: T): State[T] = this.copy(pending = pending.enqueue(task))

  def dequeue(): Option[(T, State[T])] = Option.when(pending.nonEmpty) {
    val (head, rest) = pending.dequeue
    head -> this.copy(pending = rest)
  }
}

class Var[T](var currentValue: T, private val pool: ForkJoinPool) {
  def this(currentValue: T) = this(currentValue, ForkJoinPool.commonPool())

  type MD = T => T
  private val states = new AtomicReference[State[MD]](State(None, Queue.empty))

  def submit(md: MD): Unit = add(md)

  def execute(md: MD): Future[T] = {
    val p = Promise[T]
    add { t =>
      val nv = md(t)
      p.success(nv)
      nv
    }
    p.future
  }

  def getRunning: Option[MD] = states.get().currentRun

  @tailrec
  private def add(md: MD): Unit = {
    val current = states.get()
    if (!states.compareAndSet(current, current.enqueue(md))) add(md)
    else tryRunNext()
  }

  private def tryRunNext(): Unit = if (getRunning.isEmpty) runNext(null)

  @tailrec private def runNext(previousRun: MD): Unit = {
    val curState = states.get()
    if (curState.currentRun.orNull eq previousRun) {
      curState.dequeue() match {
        case Some((nextTask, newSt)) =>
          if (!states.compareAndSet(curState, newSt.setRunning(nextTask))) runNext(previousRun) else run(nextTask)
        case None =>
          if (!states.compareAndSet(curState, curState.setIdeal())) runNext(previousRun)
      }
    }
  }

  private def run(md: MD): Unit = {
    pool.execute(() => {
      currentValue = md(currentValue)
      runNext(md)
    })
  }
}