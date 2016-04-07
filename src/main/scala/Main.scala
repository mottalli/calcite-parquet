import java.nio.file.{FileSystems, Files, Path, Paths}
import java.sql.DriverManager

import com.typesafe.scalalogging.LazyLogging
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.{DataContext, rel}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.schema._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.hadoop.fs.{Path => HDFSPath}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.tools.read.{SimpleReadSupport, SimpleRecord}

import collection.JavaConversions._

class ParquetSchemaFactory extends SchemaFactory with LazyLogging {
  override def create(parentSchema: SchemaPlus, name: String, operand: java.util.Map[String, Object]): Schema = {
    logger.debug(s"Creating ParquetSchemaFactory with name '$name'")
    val path = Paths.get(operand.get("directory").asInstanceOf[String])
    new ParquetSchema(path)
  }
}

class ParquetSchema(val path: Path) extends AbstractSchema with LazyLogging {
  val parquetFiles = Files.newDirectoryStream(path).iterator().filter(_.toString.endsWith(".parquet"))

  val tables: Iterator[(String, Table)] = parquetFiles.map { path =>
    val basename = path.getFileName.toString
    val idx = basename.lastIndexOf(".")
    val tableName = basename.substring(0, idx).toUpperCase
    logger.debug(s"Creating table '$tableName' from $path")
    val table = new ParquetTable(path)
    tableName -> table
  }

  val tablesMap = Map[String, Table](tables.toSeq: _*)

  override protected def getTableMap: java.util.Map[String, Table] = tablesMap
}

class ParquetTable(path: Path) extends AbstractTable with ScannableTable with LazyLogging {
  val hdfsPath = new HDFSPath(path.toString)

  logger.debug(s"Reading Parquet schema from $path")
  val config = new org.apache.hadoop.conf.Configuration()
  val metadata = org.apache.parquet.hadoop.ParquetFileReader.readFooter(config, hdfsPath, ParquetMetadataConverter.NO_FILTER)
  val messageType = metadata.getFileMetaData.getSchema
  val columns = messageType.getColumns

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val fieldInfoBuilder = new RelDataTypeFactory.FieldInfoBuilder(typeFactory)
    columns.foreach { descriptor =>
      assert(descriptor.getPath.length == 1)
      val name = descriptor.getPath.apply(0).toUpperCase

      val columnType = descriptor.getType match {
        case PrimitiveTypeName.FLOAT => typeFactory.createSqlType(SqlTypeName.FLOAT)
        case PrimitiveTypeName.BINARY => typeFactory.createSqlType(SqlTypeName.BINARY)
        case PrimitiveTypeName.INT32 => typeFactory.createSqlType(SqlTypeName.INTEGER)
        case PrimitiveTypeName.INT64 => typeFactory.createSqlType(SqlTypeName.BIGINT)
        case _ => ???
      }

      //logger.debug(s"Field '$name' of type '${descriptor.getType}'")

      fieldInfoBuilder.add(name, columnType)

    }

    fieldInfoBuilder.build()
  }

  override def scan(root: DataContext): Enumerable[Array[Object]] = new AbstractEnumerable[Array[Object]] {
    override def enumerator(): Enumerator[Array[Object]] = {
      new Enumerator[Array[Object]] {

        val reader = ParquetReader.builder(new SimpleReadSupport(), hdfsPath).build()
        var currentRow: Option[SimpleRecord] = None

        override def current(): Array[Object] = {
          val values = currentRow.get.getValues
          currentRow.get.getValues.foreach(nv => println(s"${nv.getName} -> ${nv.getValue}"))
          println(s"NUMBER OF VALUES: ${values.length}")
          values.map(_.getValue).toArray
        }
        override def reset() = ()
        override def close() = ()
        override def moveNext(): Boolean = {
          currentRow = Option(reader.read())
          currentRow.isDefined
        }
      }
    }
  }
}

object Main extends App {
    //new ParquetSchema()
  val jsonPath = getClass.getResource("test-model.json").getPath
  val url = s"jdbc:calcite:model=$jsonPath"

  val conn = DriverManager.getConnection(url)

  val rs = conn.createStatement().executeQuery("SELECT SUM(CLIENTS.INCOME) FROM CLIENTS")

  rs.next()
  val result = rs.getLong(1)
  println(s"Result: $result")
}
