package strom

import java.io.{DataInputStream, FileInputStream, BufferedInputStream, File}

/** A minimal class file reader: method names and bytecode lengths (the `code_length` of each
 *  method's Code attribute), to check generated methods against HotSpot's 8000-byte limit. */
object ClassFiles:
  final case class MethodSize(cls: String, name: String, bytes: Int)

  def methodSizes(f: File): List[MethodSize] =
    val in = DataInputStream(BufferedInputStream(FileInputStream(f)))
    try
      require(in.readInt() == 0xCAFEBABE)
      in.readUnsignedShort(); in.readUnsignedShort()
      val cpCount = in.readUnsignedShort()
      val utf8 = new Array[String](cpCount)
      var i = 1
      while i < cpCount do
        in.readUnsignedByte() match
          case 1 => utf8(i) = in.readUTF()
          case 3 | 4 => in.readInt()
          case 5 | 6 => in.readLong(); i += 1
          case 7 | 8 | 16 | 19 | 20 => in.readUnsignedShort()
          case 9 | 10 | 11 | 12 | 17 | 18 => in.readInt()
          case 15 => in.readUnsignedByte(); in.readUnsignedShort()
          case t => sys.error(s"bad constant pool tag $t in $f")
        i += 1
      in.readUnsignedShort(); in.readUnsignedShort(); in.readUnsignedShort()
      for _ <- 0 until in.readUnsignedShort() do in.readUnsignedShort()
      def skipAttributes(): Unit =
        for _ <- 0 until in.readUnsignedShort() do
          in.readUnsignedShort(); in.skipNBytes(in.readInt().toLong & 0xffffffffL)
      for _ <- 0 until in.readUnsignedShort() do // fields
        in.skipNBytes(6); skipAttributes()
      val methods = List.newBuilder[MethodSize]
      for _ <- 0 until in.readUnsignedShort() do
        in.readUnsignedShort()
        val name = utf8(in.readUnsignedShort())
        in.readUnsignedShort()
        for _ <- 0 until in.readUnsignedShort() do
          val attr = utf8(in.readUnsignedShort())
          val len = in.readInt()
          if attr == "Code" then
            in.readUnsignedShort(); in.readUnsignedShort()
            val codeLen = in.readInt()
            methods += MethodSize(f.getName, name, codeLen)
            in.skipNBytes(len - 8L)
          else in.skipNBytes(len.toLong)
      methods.result()
    finally in.close()

  def all(dir: File): List[MethodSize] =
    def walk(f: File): List[File] = if f.isDirectory then f.listFiles.toList.flatMap(walk) else List(f)
    walk(dir).filter(_.getName.endsWith(".class")).flatMap(methodSizes)

  def testClassesDir: File =
    File(classOf[SegmentSuite].getProtectionDomain.getCodeSource.getLocation.toURI)
