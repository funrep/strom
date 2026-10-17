package iofuse.bench

import cats.effect.unsafe.implicits.global

/** Runs one program in a loop, for JFR allocation profiling: AllocMain <name> <plain|fused>. */
@main def allocMain(name: String, mode: String): Unit =
  val (_, plain, fused) = Programs.all.find(_._1 == name).get
  val p = if mode == "plain" then plain else fused
  val end = System.nanoTime() + 8_000_000_000L
  var n = 0L
  while System.nanoTime() < end do { p().unsafeRunSync(); n += 1 }
  println(n)
