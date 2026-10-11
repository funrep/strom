package strom.bench

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import org.openjdk.jmh.annotations.*
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import strom.Strom
import strom.fs2.StromFs2
import strom.loom.{LoomParallel, StromLoom}
import strom.loom.StromLoom.*

/** 1..N core scaling of a replicated (stateless) tier: ~20 arithmetic ops per element over
 *  2^22 Ints in chunks of 8192. Per element. */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(Scaling.N)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
class Scaling:
  import Scaling.*

  @Param(Array("1", "2", "4", "8"))
  var parallelism: Int = 0

  val data: Array[Int] = Array.tabulate(N)(i => (i * 2654435761L).toInt)
  val chunks: Vector[Array[Int]] = data.grouped(ChunkSize).toVector
  val fs2Chunks = chunks.map(_root_.fs2.Chunk.array(_))

  inline def work(a: Strom[Int]) =
    a.map(x => x * 31 + 7).map(x => x ^ (x >>> 13)).map(x => x * 0x5bd1e995).map(x => x ^ (x >>> 15))
     .filter(_ % 3 != 0).map(x => x * 17 + (x >>> 7)).map(x => x ^ (x << 5)).map(x => (x * 13) ^ (x >>> 11))

  val plan = LoomParallel.plan((a: Strom[Int]) => work(a))
  val single = Strom.segment((a: Strom[Int]) => work(a))

  @Benchmark def loom(): Long =
    val in = ArrayBlockingQueue[Msg](64)
    val out = ArrayBlockingQueue[Msg](64)
    val running = LoomParallel.start(plan, in, out, LoomParallel.Config(parallelism = parallelism,
      compute = Thread.ofPlatform().daemon().factory()))
    Thread.ofVirtual().start { () => chunks.foreach(c => in.put(Data(c, 0, c.length))); in.put(End) }
    var sum = 0L
    var done = false
    while !done do
      out.take() match
        case Data(a, off, len) => sum += len
        case End => done = true
        case Failed(e) => throw e
        case _ => ()
    running.join()
    sum

  @Benchmark def fs2(): Long =
    val in = _root_.fs2.Stream.emits(fs2Chunks).flatMap(_root_.fs2.Stream.chunk).covary[IO]
    StromFs2.fusedPar(parallelism)(
      in.map(x => x * 31 + 7).map(x => x ^ (x >>> 13)).map(x => x * 0x5bd1e995).map(x => x ^ (x >>> 15))
        .filter(_ % 3 != 0).map(x => x * 17 + (x >>> 7)).map(x => x ^ (x << 5)).map(x => (x * 13) ^ (x >>> 11))
    ).chunks.map(_.size.toLong).compile.foldMonoid.unsafeRunSync()

  val zioChunks = chunks.map(zio.Chunk.fromArray(_))

  @Benchmark def zioPar(): Long =
    val in = zio.stream.ZStream.fromChunks(zioChunks*)
    val eff = strom.zio.StromZio.fusedPar(parallelism)(
      in.map(x => x * 31 + 7).map(x => x ^ (x >>> 13)).map(x => x * 0x5bd1e995).map(x => x ^ (x >>> 15))
        .filter(_ % 3 != 0).map(x => x * 17 + (x >>> 7)).map(x => x ^ (x << 5)).map(x => (x * 13) ^ (x >>> 11))
    ).chunks.map(_.size.toLong).runSum
    zio.Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.run(eff).getOrThrow())

  /** Sequential reference point: the same segment on one thread (independent of `parallelism`). */
  @Benchmark def sequential(): Long =
    val seg = single.create(1024, 1 << 16)
    var n = 0L
    chunks.foreach(c => n += strom.runtime.Chunks.processStateless(seg, strom.runtime.ArrayChunk(c, 0, c.length)).length)
    n

object Scaling:
  final val N = 1 << 22
  final val ChunkSize = 8192
