import java.nio.file.{FileSystems, Files, Path, Paths}
import java.sql.DriverManager

import com.typesafe.scalalogging.LazyLogging
import org.apache.calcite.adapter.enumerable.EnumerableRel.{Prefer, Result}
import org.apache.calcite.adapter.enumerable._
import org.apache.calcite.linq4j._
import org.apache.calcite.linq4j.tree.{Blocks, Expressions}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.{DataContext, rel}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.schema._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.hadoop.fs.{Path => HDFSPath}
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.tools.read.{SimpleReadSupport, SimpleRecord}
import org.slf4j.bridge.SLF4JBridgeHandler

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
    //val table = new ParquetScannableTable(path)
    val table = new ParquetTranslatableTable(path)
    tableName -> table
  }

  val tablesMap = Map[String, Table](tables.toSeq: _*)

  override protected def getTableMap: java.util.Map[String, Table] = tablesMap
}

class ParquetEnumerator(table: ParquetTable) extends Enumerator[Array[Object]] {
  val reader = ParquetReader.builder(new SimpleReadSupport, table.hdfsPath).build()
  var currentRow: Option[SimpleRecord] = None

  override def moveNext(): Boolean = {
    currentRow = Option(reader.read())
    currentRow.isDefined
  }

  val numColumns = table.columns.length
  val resultRow = collection.mutable.ArrayBuffer.fill[Object](numColumns)(null)

  override def current(): Array[Object] = {
    val definedValues = currentRow.get.getValues

    for (i <- 0 until numColumns)
      resultRow(i) = null

    for (nv <- definedValues)
      resultRow(table.columnsIndexes.get(nv.getName).get) = nv.getValue

    resultRow.toArray
  }

  override def reset() = {}
  override def close() = {}
}

class ParquetEnumerable(table: ParquetTable) extends AbstractEnumerable[Array[Object]] {
  override def enumerator(): Enumerator[Array[Object]] = new ParquetEnumerator(table)

}

class ParquetTable(path: Path) extends AbstractTable with LazyLogging {
  val hdfsPath = new HDFSPath(path.toString)

  val columns = {
    val config = new org.apache.hadoop.conf.Configuration()
    logger.debug(s"Reading Parquet schema from $path")
    val metadata = org.apache.parquet.hadoop.ParquetFileReader.readFooter(config, hdfsPath, ParquetMetadataConverter.NO_FILTER)
    val messageType = metadata.getFileMetaData.getSchema
    messageType.getColumns
  }

  columns.foreach(c => logger.debug(s"Got column: $c"))

  val columnsIndexes: Map[String, Int] = Map(columns.map(_.getPath()(0)).zipWithIndex.map(x => x._1 -> x._2): _*)

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val fieldInfoBuilder = new RelDataTypeFactory.FieldInfoBuilder(typeFactory)
    columns.foreach { descriptor =>
      assert(descriptor.getPath.length == 1)
      val name = descriptor.getPath.apply(0).toUpperCase

      val columnType = descriptor.getType match {
        case PrimitiveTypeName.FLOAT => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.FLOAT), true)
        case PrimitiveTypeName.BINARY => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true)
        case PrimitiveTypeName.INT32 => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        case PrimitiveTypeName.INT64 => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        case _ => ???
      }

      //logger.debug(s"Field '$name' of type '${descriptor.getType}'")

      fieldInfoBuilder.add(name, columnType)
    }

    fieldInfoBuilder.build()
  }
}

class ParquetScannableTable(path: Path) extends ParquetTable(path) with ScannableTable {
  override def scan(root: DataContext): Enumerable[Array[Object]] = new ParquetEnumerable(this)
}

class ParquetTranslatableTable(path: Path) extends ParquetTable(path) with QueryableTable with TranslatableTable {
  override def asQueryable[T](queryProvider: QueryProvider, schema: SchemaPlus, tableName: String): Queryable[T] = {
    logger.info("AS QUERYABLE")
    ???
  }

  override def toRel(context: RelOptTable.ToRelContext, relOptTable: RelOptTable): RelNode = {
    logger.info("TO REL")
    //Thread.currentThread().getStackTrace.foreach(println)
    return new ParquetTableScan(context.getCluster, relOptTable)
  }

  override def getExpression(schema: SchemaPlus, tableName: String, clazz: java.lang.Class[_]): org.apache.calcite.linq4j.tree.Expression = {
    logger.info("GET EXPRESSION")
    Schemas.tableExpression(schema, getElementType(), tableName, clazz)
  }

  override def getElementType(): java.lang.reflect.Type = {
    classOf[Array[Object]]
  }

  def project(): Enumerable[Array[Object]] = {
    val table = this
    new AbstractEnumerable[Array[Object]] {
      override def enumerator(): Enumerator[Array[Object]] = new ParquetEnumerator(table)
    }
  }
}

class ParquetTableScan(cluster: RelOptCluster, tablex: RelOptTable)
  extends TableScan(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), tablex)
  with EnumerableRel
  with LazyLogging {

  override def implement(implementor: EnumerableRelImplementor, pref: Prefer): Result = {
    logger.info("IMPLEMENT")

    val physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType, pref.preferArray())
    implementor.result(
      physType,
      Blocks.toBlock(
        Expressions.call(table.getExpression(classOf[ParquetTranslatableTable]), "project")
      )
    )
  }
}

object Main extends App {
  SLF4JBridgeHandler.removeHandlersForRootLogger()
  SLF4JBridgeHandler.install()

    //new ParquetSchema()
  val jsonPath = getClass.getResource("test-model.json").getPath
  val url = s"jdbc:calcite:model=$jsonPath"

  val conn = DriverManager.getConnection(url)

  val rs = conn.createStatement().executeQuery(
    "SELECT SOCIABILITY_INDEX AS SI, SUM(PERSON_ID) AS TUTTI " +
    "FROM CLIENTS AS CX " +
    "WHERE MDN IS NOT NULL AND NUM_TRANSACTIONS > 0" +
    "GROUP BY SOCIABILITY_INDEX " +
    "LIMIT 10")

  val numColumns = rs.getMetaData.getColumnCount
  while (rs.next()) {
    val row = (1 to numColumns).map(rs.getObject)
    println(s"Row: $row")
  }
}
