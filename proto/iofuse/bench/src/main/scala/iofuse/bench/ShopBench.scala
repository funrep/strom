package iofuse.bench

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import iofuse.IOFuse
import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit

/** A more realistic program than [[Programs]]: an order checkout service written the usual way.
 *  Per order: validation, a user lookup (a service call), pricing each line item through a service
 *  (a recursive local def), reserving stock in a `Ref` (can be rejected), recording the sale. The
 *  driver is ordinary `traverse` code and is not fused; only the handler body is inside `fused`,
 *  which is where a user would put it. Scores are per order. */
object Shop:
  final case class Item(sku: Int, qty: Int)
  final case class Order(id: Int, user: Int, items: List[Item], coupon: Option[String])
  final case class User(id: Int, vip: Boolean)
  final case class Receipt(order: Int, total: Long, discount: Long)
  final case class Summary(ok: Int, rejected: Int, revenue: Long)
  final case class Rejected(reason: String) extends RuntimeException(reason, null, false, false)

  final val Orders = 1000
  // 2% empty orders, 1 in 6 users unknown, some SKUs out of stock: about a quarter are rejected.
  val orders: Vector[Order] = Vector.tabulate(Orders) { i =>
    val n = if i % 50 == 0 then 0 else 1 + i % 4
    Order(i, i % 120, List.tabulate(n)(k => Item((i * 7 + k * 13) % 200, 1 + (i + k) % 3)),
      if i % 3 == 0 then Some("SPRING") else None)
  }
  val userTable: Map[Int, User] = (0 until 100).map(i => i -> User(i, i % 7 == 0)).toMap
  val priceTable: Array[Long] = Array.tabulate(200)(s => 100L + s * 37 % 900)
  val initialStock: Map[Int, Int] = (0 until 200).map(s => s -> (if s % 41 == 0 then 0 else 1000000)).toMap

  def summarize(rs: Iterable[Either[Throwable, Receipt]]): Summary =
    rs.foldLeft(Summary(0, 0, 0L)) {
      case (s, Right(r))          => s.copy(ok = s.ok + 1, revenue = s.revenue + r.total)
      case (s, Left(_: Rejected)) => s.copy(rejected = s.rejected + 1)
      case (_, Left(e))           => throw e
    }

// ---- cats-effect ----

final class Services(stock: Ref[IO, Map[Int, Int]], sold: Ref[IO, Long], blockingLookups: Boolean):
  import Shop.*
  def findUser(id: Int): IO[Option[User]] =
    if blockingLookups then IO.blocking(userTable.get(id)) else IO(userTable.get(id))
  def price(sku: Int): IO[Long] = IO(priceTable(sku))
  def reserve(sku: Int, qty: Int): IO[Boolean] = stock.modify { m =>
    val have = m(sku)
    if have >= qty then (m.updated(sku, have - qty), true) else (m, false)
  }
  def recordSale(amount: Long): IO[Unit] = sold.update(_ + amount)

object Services:
  def make(blockingLookups: Boolean): IO[Services] =
    for
      stock <- Ref[IO].of(Shop.initialStock)
      sold  <- Ref[IO].of(0L)
    yield Services(stock, sold, blockingLookups)

trait Checkout:
  import Shop.*
  def checkout(o: Order): IO[Receipt]
  def runAll: IO[Summary] = orders.traverse(o => checkout(o).attempt).map(summarize)
  def runPar: IO[Summary] =
    orders.grouped(Orders / 4).toVector.parTraverse(_.traverse(o => checkout(o).attempt)).map(g => summarize(g.flatten))

final class PlainCheckout(s: Services) extends Checkout:
  import Shop.*
  def checkout(o: Order): IO[Receipt] =
    def priceAll(items: List[Item], acc: Long): IO[Long] = items match
      case Nil        => IO.pure(acc)
      case it :: rest => s.price(it.sku).flatMap(p => priceAll(rest, acc + p * it.qty))
    def reserveAll(items: List[Item]): IO[Unit] = items match
      case Nil => IO.unit
      case it :: rest =>
        s.reserve(it.sku, it.qty).flatMap(ok => if ok then reserveAll(rest) else IO.raiseError(Rejected(s"out of stock: ${it.sku}")))
    for
      _ <- if o.items.isEmpty then IO.raiseError(Rejected("empty order")) else IO.unit
      user <- s.findUser(o.user).flatMap {
        case Some(u) => IO.pure(u)
        case None    => IO.raiseError(Rejected(s"unknown user ${o.user}"))
      }
      total <- priceAll(o.items, 0L)
      discount = o.coupon match
        case Some(_) if user.vip => total / 10
        case Some(_)             => total / 20
        case None                => 0L
      _ <- reserveAll(o.items)
      _ <- s.recordSale(total - discount)
    yield Receipt(o.id, total - discount, discount)

final class FusedCheckout(s: Services) extends Checkout:
  import Shop.*
  def checkout(o: Order): IO[Receipt] = IOFuse.fused {
    def priceAll(items: List[Item], acc: Long): IO[Long] = items match
      case Nil        => IO.pure(acc)
      case it :: rest => s.price(it.sku).flatMap(p => priceAll(rest, acc + p * it.qty))
    def reserveAll(items: List[Item]): IO[Unit] = items match
      case Nil => IO.unit
      case it :: rest =>
        s.reserve(it.sku, it.qty).flatMap(ok => if ok then reserveAll(rest) else IO.raiseError(Rejected(s"out of stock: ${it.sku}")))
    for
      _ <- if o.items.isEmpty then IO.raiseError(Rejected("empty order")) else IO.unit
      user <- s.findUser(o.user).flatMap {
        case Some(u) => IO.pure(u)
        case None    => IO.raiseError(Rejected(s"unknown user ${o.user}"))
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

object ShopIO:
  def seq(fused: Boolean, blocking: Boolean = false): IO[Shop.Summary] =
    Services.make(blocking).flatMap(s => (if fused then FusedCheckout(s) else PlainCheckout(s)).runAll)
  def par(fused: Boolean): IO[Shop.Summary] =
    Services.make(false).flatMap(s => (if fused then FusedCheckout(s) else PlainCheckout(s)).runPar)

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = Array("-Xms1g", "-Xmx1g"))
@OperationsPerInvocation(Shop.Orders)
class ShopBench:
  @Benchmark def seqPlain(): Shop.Summary      = ShopIO.seq(false).unsafeRunSync()
  @Benchmark def seqFused(): Shop.Summary      = ShopIO.seq(true).unsafeRunSync()
  @Benchmark def parPlain(): Shop.Summary      = ShopIO.par(false).unsafeRunSync()
  @Benchmark def parFused(): Shop.Summary      = ShopIO.par(true).unsafeRunSync()
  @Benchmark def blockingPlain(): Shop.Summary = ShopIO.seq(false, blocking = true).unsafeRunSync()
  @Benchmark def blockingFused(): Shop.Summary = ShopIO.seq(true, blocking = true).unsafeRunSync()
