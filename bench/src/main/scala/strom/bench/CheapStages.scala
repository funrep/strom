package strom.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.*
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import strom.Strom
import strom.fs2.StromFs2
import strom.zio.StromZio
import strom.runtime.Runner

/** Eight cheap stages over 2^20 Ints (map/filter/…/sum): the per-element cost of the
 *  pipeline machinery itself. Results are per element (OperationsPerInvocation). */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(CheapStages.N)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class CheapStages:
  import CheapStages.*

  val data: Array[Int] = Array.tabulate(N)(i => (i * 2654435761L).toInt >>> 8)
  val chunks: Vector[Array[Int]] = data.grouped(ChunkSize).toVector
  val fs2Chunks: Vector[_root_.fs2.Chunk[Int]] = chunks.map(_root_.fs2.Chunk.array(_))
  val zioChunks: Vector[zio.Chunk[Int]] = chunks.map(zio.Chunk.fromArray(_))
  val segment = Strom.segment((a: Strom[Int]) => stages(a))

  inline def stages(s: Strom[Int]) =
    s.map(_ * 3).filter(_ % 4 != 1).map(_ + 7).map(x => x ^ (x >>> 3)).filter(_ % 3 != 0).map(_ * 2).map(_ - 1).sum

  @Benchmark def whileLoop(): Int =
    var acc = 0; var i = 0
    while i < data.length do
      val a = data(i) * 3
      if a % 4 != 1 then
        val c = a + 7; val d = c ^ (c >>> 3)
        if d % 3 != 0 then acc += d * 2 - 1
      i += 1
    acc

  @Benchmark def stromDirect(): Int = Strom.run(stages(Strom.fromArray(data)))

  @Benchmark def stromRunner(): Int =
    val r = Runner(segment, maxOut = 1, budget = 1 << 16)
    var i = 0
    while r.next() != Runner.Done do
      if i < chunks.length then { r.offer(0, chunks(i), 0, chunks(i).length); i += 1 } else r.endOfInput(0)
    r.result

  @Benchmark def stromFs2(): Int =
    _root_.fs2.Stream.emits(fs2Chunks).flatMap(_root_.fs2.Stream.chunk).covary[IO]
      .through(StromFs2.fold[IO, Int, Int](segment)).compile.lastOrError.unsafeRunSync()

  @Benchmark def stromZio(): Int =
    val eff = StromZio.runFold[Any, Nothing, Int, Int](segment)(zio.stream.ZStream.fromChunks(zioChunks*))
    zio.Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.run(eff).getOrThrow())

  /** The plain fs2 code below, wrapped in `StromFs2.fused`. */
  @Benchmark def fs2Fused(): Int =
    StromFs2.fused(_root_.fs2.Stream.emits(fs2Chunks).flatMap(_root_.fs2.Stream.chunk).covary[IO]
      .map(_ * 3).filter(_ % 4 != 1).map(_ + 7).map(x => x ^ (x >>> 3)).filter(_ % 3 != 0).map(_ * 2).map(_ - 1)
      .fold(0)(_ + _)).compile.lastOrError.unsafeRunSync()

  @Benchmark def fs2Plain(): Int =
    _root_.fs2.Stream.emits(fs2Chunks).flatMap(_root_.fs2.Stream.chunk).covary[IO]
      .map(_ * 3).filter(_ % 4 != 1).map(_ + 7).map(x => x ^ (x >>> 3)).filter(_ % 3 != 0).map(_ * 2).map(_ - 1)
      .compile.fold(0)(_ + _).unsafeRunSync()

  @Benchmark def zstream(): Int =
    val eff = zio.stream.ZStream.fromChunks(zioChunks*)
      .map(_ * 3).filter(_ % 4 != 1).map(_ + 7).map(x => x ^ (x >>> 3)).filter(_ % 3 != 0).map(_ * 2).map(_ - 1)
      .runSum
    zio.Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.run(eff).getOrThrow())

  @Benchmark def scalaIterator(): Int =
    data.iterator.map(_ * 3).filter(_ % 4 != 1).map(_ + 7).map(x => x ^ (x >>> 3)).filter(_ % 3 != 0).map(_ * 2).map(_ - 1).sum

object CheapStages:
  final val N = 1 << 20
  final val ChunkSize = 4096
