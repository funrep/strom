package strom.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.*
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import strom.{Pipelines, Strom}
import strom.fs2.StromFs2
import strom.zio.StromZio
import strom.runtime.Runner

/** zip(a.flatMap(x => range(0, x)), b.filter(even)).map(*).take(n).sum — nested + zip + take.
 *  Per element of output (OperationsPerInvocation = n). */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(Representative.N)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class Representative:
  import Representative.*

  val a: Array[Int] = Array.tabulate(N)(i => i % 9)          // ~4 inner elements per a-element
  val b: Array[Int] = Array.tabulate(4 * N)(i => i % 1000)   // half of them pass the filter
  def fs2Of(xs: Array[Int]) = _root_.fs2.Stream.emits(xs.grouped(4096).toVector.map(_root_.fs2.Chunk.array(_)))
    .flatMap(_root_.fs2.Stream.chunk).covary[IO]
  def zioOf(xs: Array[Int]) = zio.stream.ZStream.fromChunks(xs.grouped(4096).toVector.map(zio.Chunk.fromArray(_))*)
  val segment = Strom.segment(Pipelines.representative(N))

  /** The same pipeline written by hand: the inner range's position and the filtered side's
   *  cursor kept in locals. */
  @Benchmark def whileLoop(): Int =
    var acc = 0; var n = 0
    var ia = 0; var j = 0; var lim = 0 // left: a(ia - 1) expanded to 0 until lim
    var ib = 0                         // right: next index into b
    var going = true
    while going && n < N do
      while j >= lim && ia < a.length do { lim = a(ia); ia += 1; j = 0 }
      if j >= lim then going = false
      else
        val x = j; j += 1
        while ib < b.length && b(ib) % 2 != 0 do ib += 1
        if ib >= b.length then going = false
        else { acc += x * b(ib); ib += 1; n += 1 }
    acc

  @Benchmark def stromDirect(): Int =
    Strom.run(Strom.fromArray(a).flatMap(x => Strom.range(0, x))
      .zipWith(Strom.fromArray(b).filter(y => y % 2 == 0))(_ * _).take(N).sum)

  @Benchmark def stromRunner(): Int =
    val r = Runner(segment, maxOut = 1, budget = 1 << 16)
    var pa = 0; var pb = 0
    var s = r.next()
    while s != Runner.Done do
      s match
        case Runner.NeedInput(0) => if pa < a.length then { r.offer(0, a, pa, math.min(4096, a.length - pa)); pa += 4096 } else r.endOfInput(0)
        case Runner.NeedInput(_) => if pb < b.length then { r.offer(1, b, pb, math.min(4096, b.length - pb)); pb += 4096 } else r.endOfInput(1)
        case _ => ()
      s = r.next()
    r.result

  @Benchmark def stromFs2(): Int =
    StromFs2.fold2[IO, Int, Int, Int](segment)(fs2Of(a), fs2Of(b)).compile.lastOrError.unsafeRunSync()

  @Benchmark def fs2Fused(): Int =
    StromFs2.fused(fs2Of(a).flatMap(x => _root_.fs2.Stream.range(0, x)).zipWith(fs2Of(b).filter(_ % 2 == 0))(_ * _)
      .take(N.toLong).fold(0)(_ + _)).compile.lastOrError.unsafeRunSync()

  @Benchmark def fs2FusedPar4(): Int =
    StromFs2.fusedPar(4)(fs2Of(a).flatMap(x => _root_.fs2.Stream.range(0, x)).zipWith(fs2Of(b).filter(_ % 2 == 0))(_ * _)
      .take(N.toLong).fold(0)(_ + _)).compile.lastOrError.unsafeRunSync()

  @Benchmark def zioFused(): Int =
    val eff = StromZio.fused(zioOf(a).flatMap(x => zio.stream.ZStream.range(0, x)).zipWith(zioOf(b).filter(_ % 2 == 0))(_ * _)
      .take(N.toLong)).runSum
    zio.Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.run(eff).getOrThrow())

  @Benchmark def zioFusedPar4(): Int =
    val eff = StromZio.fusedPar(4)(zioOf(a).flatMap(x => zio.stream.ZStream.range(0, x)).zipWith(zioOf(b).filter(_ % 2 == 0))(_ * _)
      .take(N.toLong)).runSum
    zio.Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.run(eff).getOrThrow())

  @Benchmark def fs2Plain(): Int =
    fs2Of(a).flatMap(x => _root_.fs2.Stream.range(0, x)).zipWith(fs2Of(b).filter(_ % 2 == 0))(_ * _)
      .take(N.toLong).compile.fold(0)(_ + _).unsafeRunSync()

  @Benchmark def zstream(): Int =
    val eff = zioOf(a).flatMap(x => zio.stream.ZStream.range(0, x)).zipWith(zioOf(b).filter(_ % 2 == 0))(_ * _)
      .take(N.toLong).runSum
    zio.Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.run(eff).getOrThrow())

object Representative:
  final val N = 1 << 18
