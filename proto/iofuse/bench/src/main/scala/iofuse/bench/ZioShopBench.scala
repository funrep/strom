package iofuse.bench

import iofuse.IOFuse
import org.openjdk.jmh.annotations.*
import zio.{Ref, ZIO}
import java.util.concurrent.TimeUnit

/** The [[Shop]] checkout service written with ZIO: rejections are typed failures. */
final class ZServices(stock: Ref[Map[Int, Int]], sold: Ref[Long], blockingLookups: Boolean):
  import Shop.*
  def findUser(id: Int): ZIO[Any, Nothing, Option[User]] =
    if blockingLookups then ZIO.blocking(ZIO.succeed(userTable.get(id))) else ZIO.succeed(userTable.get(id))
  def price(sku: Int): ZIO[Any, Nothing, Long] = ZIO.succeed(priceTable(sku))
  def reserve(sku: Int, qty: Int): ZIO[Any, Nothing, Boolean] = stock.modify { m =>
    val have = m(sku)
    if have >= qty then (true, m.updated(sku, have - qty)) else (false, m)
  }
  def recordSale(amount: Long): ZIO[Any, Nothing, Unit] = sold.update(_ + amount)

object ZServices:
  def make(blockingLookups: Boolean): ZIO[Any, Nothing, ZServices] =
    for
      stock <- Ref.make(Shop.initialStock)
      sold  <- Ref.make(0L)
    yield ZServices(stock, sold, blockingLookups)

trait ZCheckout:
  import Shop.*
  def checkout(o: Order): ZIO[Any, Rejected, Receipt]
  def runAll: ZIO[Any, Nothing, Summary] = ZIO.foreach(orders)(o => checkout(o).either).map(summarize)
  def runPar: ZIO[Any, Nothing, Summary] =
    ZIO.foreachPar(orders.grouped(Orders / 4).toVector)(g => ZIO.foreach(g)(o => checkout(o).either))
      .withParallelism(4).map(g => summarize(g.flatten))

final class ZPlainCheckout(s: ZServices) extends ZCheckout:
  import Shop.*
  def checkout(o: Order): ZIO[Any, Rejected, Receipt] =
    def priceAll(items: List[Item], acc: Long): ZIO[Any, Nothing, Long] = items match
      case Nil        => ZIO.succeed(acc)
      case it :: rest => s.price(it.sku).flatMap(p => priceAll(rest, acc + p * it.qty))
    def reserveAll(items: List[Item]): ZIO[Any, Rejected, Unit] = items match
      case Nil => ZIO.unit
      case it :: rest =>
        s.reserve(it.sku, it.qty).flatMap(ok => if ok then reserveAll(rest) else ZIO.fail(Rejected(s"out of stock: ${it.sku}")))
    for
      _ <- if o.items.isEmpty then ZIO.fail(Rejected("empty order")) else ZIO.unit
      user <- s.findUser(o.user).flatMap {
        case Some(u) => ZIO.succeed(u)
        case None    => ZIO.fail(Rejected(s"unknown user ${o.user}"))
      }
      total <- priceAll(o.items, 0L)
      discount = o.coupon match
        case Some(_) if user.vip => total / 10
        case Some(_)             => total / 20
        case None                => 0L
      _ <- reserveAll(o.items)
      _ <- s.recordSale(total - discount)
    yield Receipt(o.id, total - discount, discount)

final class ZFusedCheckout(s: ZServices) extends ZCheckout:
  import Shop.*
  def checkout(o: Order): ZIO[Any, Rejected, Receipt] = IOFuse.fused {
    def priceAll(items: List[Item], acc: Long): ZIO[Any, Nothing, Long] = items match
      case Nil        => ZIO.succeed(acc)
      case it :: rest => s.price(it.sku).flatMap(p => priceAll(rest, acc + p * it.qty))
    def reserveAll(items: List[Item]): ZIO[Any, Rejected, Unit] = items match
      case Nil => ZIO.unit
      case it :: rest =>
        s.reserve(it.sku, it.qty).flatMap(ok => if ok then reserveAll(rest) else ZIO.fail(Rejected(s"out of stock: ${it.sku}")))
    for
      _ <- if o.items.isEmpty then ZIO.fail(Rejected("empty order")) else ZIO.unit
      user <- s.findUser(o.user).flatMap {
        case Some(u) => ZIO.succeed(u)
        case None    => ZIO.fail(Rejected(s"unknown user ${o.user}"))
      }
      total <- priceAll(o.items, 0L)
      discount = o.coupon match
        case Some(_) if user.vip => total / 10
        case Some(_)             => total / 20
        case None                => 0L
      _ <- reserveAll(o.items)
      _ <- s.recordSale(total - discount)
    yield Receipt(o.id, total - discount, discount)
  }

object ShopZIO:
  def seq(fused: Boolean, blocking: Boolean = false): ZIO[Any, Nothing, Shop.Summary] =
    ZServices.make(blocking).flatMap(s => (if fused then ZFusedCheckout(s) else ZPlainCheckout(s)).runAll)
  def par(fused: Boolean): ZIO[Any, Nothing, Shop.Summary] =
    ZServices.make(false).flatMap(s => (if fused then ZFusedCheckout(s) else ZPlainCheckout(s)).runPar)

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = Array("-Xms1g", "-Xmx1g"))
@OperationsPerInvocation(Shop.Orders)
class ZioShopBench:
  import ZPrograms.run
  @Benchmark def seqPlain(): Shop.Summary      = run(ShopZIO.seq(false))
  @Benchmark def seqFused(): Shop.Summary      = run(ShopZIO.seq(true))
  @Benchmark def parPlain(): Shop.Summary      = run(ShopZIO.par(false))
  @Benchmark def parFused(): Shop.Summary      = run(ShopZIO.par(true))
  @Benchmark def blockingPlain(): Shop.Summary = run(ShopZIO.seq(false, blocking = true))
  @Benchmark def blockingFused(): Shop.Summary = run(ShopZIO.seq(true, blocking = true))
