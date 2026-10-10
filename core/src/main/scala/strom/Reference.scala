package strom

/** The naive reference semantics of [[Strom]]: a direct interpretation over iterators.
 *  Every staged pipeline must agree with this interpreter, for every input chunking. */
object Reference:
  import Strom.Node.*

  def iterator[A](s: Strom[A]): Iterator[A] = s match
    case ArrayN(a)            => a.iterator
    case RangeN(from, until)  => Iterator.range(from, until)
    case SeqN(xs)             => xs.iterator
    case MapN(up, f)          => iterator(up).map(f)
    case FilterN(up, p)       => iterator(up).filter(p)
    case FlatMapN(up, f)      => iterator(up).flatMap(a => iterator(f(a)))
    case TakeN(up, n)         => iterator(up).take(math.max(n, 0))
    case ZipWithN(l, r, f)    => iterator(l).zip(iterator(r)).map(f.tupled)
    case WindowFoldN(up, size, z, op) =>
      iterator(up).grouped(math.max(size, 1)).map(_.foldLeft(z)(op))

  def toList[A](s: Strom[A]): List[A] = iterator(s).toList

  def run[R](f: Fold[R]): R = iterator(f.src).foldLeft(f.z)(f.op)
