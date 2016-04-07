/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.ProtocolVersion
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import com.datastax.driver.core.Statement

/**
 * INTERNAL API
 */
private[cassandra] final class CassandraSession(
  system: ActorSystem, settings: CassandraPluginConfig, executionContext: ExecutionContext,
  log:             LoggingAdapter,
  metricsCategory: String,
  init:            Session => Future[_]
) {

  implicit private val ec = executionContext

  private val _underlyingSession = new AtomicReference[Future[Session]]()

  final def underlying(): Future[Session] = {

    def initialize(session: Future[Session]): Future[Session] = {
      session.flatMap { s =>
        val result = init(s)
        result.onFailure { case _ => close(s) }
        result.map(_ => s)
      }
    }

    @tailrec def setup(): Future[Session] = {
      val existing = _underlyingSession.get
      if (existing == null) {
        val s = initialize(settings.sessionProvider.connect())
        if (_underlyingSession.compareAndSet(null, s)) {
          s.foreach { ses =>
            CassandraMetricsRegistry(system).addMetrics(metricsCategory, ses.getCluster.getMetrics.getRegistry)
          }
          s.onFailure {
            case e =>
              _underlyingSession.compareAndSet(s, null)
              log.warning(
                "Failed to connect to Cassandra and initialize. It will be retried on demand. Caused by: {}",
                e.getMessage
              )
          }
          system.registerOnTermination {
            s.foreach(close)
          }
          s
        } else {
          s.foreach(close)
          setup() // recursive
        }
      } else {
        existing
      }
    }

    val existing = _underlyingSession.get
    if (existing == null)
      retry(() => setup())
    else
      existing
  }

  private def retry(setup: () => Future[Session]): Future[Session] = {
    val promise = Promise[Session]

    def tryAgain(count: Int, cause: Throwable): Unit = {
      if (count == 0)
        promise.failure(cause)
      else {
        system.scheduler.scheduleOnce(settings.connectionRetryDelay) {
          trySetup(count)
        }
      }
    }

    def trySetup(count: Int): Unit = {
      try {
        setup().onComplete {
          case Success(session) => promise.success(session)
          case Failure(cause)   => tryAgain(count - 1, cause)
        }
      } catch {
        case NonFatal(e) =>
          // this is only in case the direct calls, such as sessionProvider, throws
          log.warning(
            "Failed to initialize CassandraSession. It will be retried on demand. Caused by: {}",
            e.getMessage
          )
          promise.failure(e)
      }
    }

    trySetup(settings.connectionRetries)

    promise.future
  }

  def prepare(stmt: String): Future[PreparedStatement] =
    underlying().flatMap(_.prepareAsync(stmt))

  def executeWrite(stmt: Statement): Future[Unit] = {
    if (stmt.getConsistencyLevel == null)
      stmt.setConsistencyLevel(settings.writeConsistency)
    underlying().flatMap { s =>
      s.executeAsync(stmt).map(_ => ())
    }
  }

  def select(stmt: Statement): Future[ResultSet] = {
    if (stmt.getConsistencyLevel == null)
      stmt.setConsistencyLevel(settings.readConsistency)
    underlying().flatMap { s =>
      s.executeAsync(stmt)
    }
  }

  private def close(s: Session): Unit = {
    s.closeAsync()
    s.getCluster().closeAsync()
    CassandraMetricsRegistry(system).removeMetrics(metricsCategory)
  }

  def close(): Unit = {
    _underlyingSession.getAndSet(null) match {
      case null     =>
      case existing => existing.foreach(close)
    }
  }

  /**
   * This can only be used after successful initialization,
   * otherwise throws `IllegalStateException`.
   */
  def protocolVersion: ProtocolVersion =
    underlying().value match {
      case Some(Success(s)) => s.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
      case _                => throw new IllegalStateException("protocolVersion can only be accessed after successful init")
    }

}

/**
 * INTERNAL API
 */
private[cassandra] final object CassandraSession {
  val createKeyspaceAndTablesSemaphore = new Semaphore(1)
}
