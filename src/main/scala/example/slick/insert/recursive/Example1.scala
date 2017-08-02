package example.slick.insert.recursive

import example.slick.insert.recursive.Example1.Clause
import org.slf4j.LoggerFactory

object Example1 {

  private final val logger = LoggerFactory.getLogger(this.getClass)
  val profile = slick.jdbc.H2Profile

  import profile.api._

  import scala.concurrent.ExecutionContext.Implicits.global

  /** A clause of type: `field1 == val1 AND field2 < val3 (field3 != val3 OR field4 >= val4)`
    */
  case class Clause(operator: String,
                    field: Option[String],
                    value: Option[String],
                    parentClause: Option[Long],
                    id: Long = 0)

  class ClauseTable(tag: Tag) extends Table[Clause](tag, "CLAUSE") {

    def id = column[Long]("id", O.AutoInc, O.PrimaryKey)

    def operator = column[String]("operator")

    def field = column[Option[String]]("field")

    def value = column[Option[String]]("value")

    def parentClause = column[Option[Long]]("parentClause")

    def parent = foreignKey("to_parent", parentClause, clauses)(_.id)

    def * = (operator, field, value, parentClause, id).mapTo[Clause]
  }

  lazy val clauses = TableQuery[ClauseTable]
  lazy val insertClause = clauses returning clauses.map(_.id)

  lazy val ddl = clauses.schema

  def insertClause(clause: Clause, parentId: Option[Long]): DBIO[Long] =
    insertClause += clause.copy(parentClause = parentId)

  /** Insert the Spec records in the list order.
    * The Spec are assumed to be in DFS (Deep First Search) order i.e a to be inserted clause of the form:
    *
    * {{{
    *  ... WHERE field1 eq val1 AND field2 lt val2 AND (field3 gt val3 OR field4 eq val4)
    * }}}
    *
    * which is equivalent to:
    *
    * {{{
    *   and(  and(eq(val1,field1), lt(field2,val2)) , or(gt(field3,val3), eq(field4,val4))  )
    * }}}
    *
    * are inserted as a (for a set type = target and mode = READ model clause), for example:
    *
    * {{{
    * Seq(
    *   AND    None     None
    *   AND    None     None
    *   EQ     field1   val1
    *   LT     field2   val2
    *   OR     None     None
    *   GT     field4   val4
    *   EQ     field5   val5
    * )
    * }}}
    *
    * @param cs clause to insert
    * @return an action to execute for all the records to be inserted.
    */
  type ACC = (Long, Option[Long], Option[Long])

  def insertClauses(cs: Seq[Clause]): DBIO[Long] = {

    def loop(acc: List[ACC], cs: Seq[Clause]): DBIO[Long] = {
      logger.debug(s"Want to insert: ${cs.head}")
      val _2ins = cs.head

      _2ins.operator match {
        case "NEQ" | "EQ" | "LT" | "GT" | "LTE" | "GTE" => {
          acc match {
            case Nil => insertClause(_2ins, None)
            case _ => {
              acc.head match {
                case (_, None, None) => {
                  debug(acc)
                  insertClause(_2ins, Some(acc.head._1)) flatMap
                    (rec => loop((acc.head._1, Some(rec), None) :: acc.tail, cs.tail))
                }

                case (_, Some(_), None) => {
                  debug(acc)
                  if (cs.tail.isEmpty) insertClause(_2ins, Some(acc.head._1))
                  else insertClause(_2ins, Some(acc.head._1)) >> loop(acc.tail, cs.tail)
                }

                case _ => throw new RuntimeException(s"Impossible case !")
              }
            }
          }
        }
        case "AND" | "OR" => {
          acc match {
            case Nil => {
              debug(acc)
              if (cs.tail.isEmpty) insertClause(_2ins, None)
              else insertClause(_2ins, None) flatMap (rec => loop((rec, None, None) :: acc, cs.tail))
            }

            case _ => {
              val h = acc.head
              h match {
                case (_, None, None) => {
                  debug(acc)
                  insertClause(_2ins, Some(h._1)).flatMap {
                    r => loop((r, None, None) :: (h._1, Some(r), None) :: acc.tail, cs.tail)
                  }
                }
                case (_, Some(_), None) => {
                  debug(acc)
                  insertClause(_2ins, Some(h._1)) flatMap (r => loop((r, None, None) :: acc.tail, cs.tail))
                }
                case _ => throw new RuntimeException(s"Impossible case !")
              }
            }
          }
        }
        case otherOp => throw new RuntimeException(s"The operator [$otherOp] not implemented.")
      }

    }

    if (cs.isEmpty) DBIO.successful(0)
    else loop(List.empty[ACC], cs)
  }


  val clause1 = Seq(Clause("EQ", Some("fields1"), Some("val1"), None))
  val clause3 = Seq(
    Clause("AND", None, None, None),
    Clause("EQ", Some("fields1"), Some("val1"), None),
    Clause("LT", Some("fields2"), Some("val2"), None)
  )

  val clause5 = Seq(
    Clause("OR", None, None, None),
    Clause("AND", None, None, None),
    Clause("EQ", Some("fields1"), Some("val1"), None),
    Clause("LT", Some("fields2"), Some("val2"), None),
    Clause("GT", Some("fields3"), Some("val3"), None)
  )

  val clause7 = Seq(
    Clause("OR", None, None, None),
    Clause("AND", None, None, None),
    Clause("EQ", Some("fields1"), Some("val1"), None),
    Clause("LT", Some("fields2"), Some("val2"), None),
    Clause("OR", None, None, None),
    Clause("GT", Some("fields3"), Some("val3"), None),
    Clause("NEQ", Some("fields4"), Some("val4"), None)
  )

  val clause13 = Seq(
    Clause("AND", None, None, None),
    Clause("AND", None, None, None),
    Clause("OR", None, None, None),
    Clause("EQ", Some("fields1"), Some("val1"), None),
    Clause("LT", Some("fields2"), Some("val2"), None),
    Clause("AND", None, None, None),
    Clause("OR", None, None, None),
    Clause("LTE", Some("fields3"), Some("val3"), None),
    Clause("GT", Some("fields4"), Some("val4"), None),
    Clause("NEQ", Some("fields5"), Some("val5"), None),
    Clause("OR", None, None, None),
    Clause("GTE", Some("fields6"), Some("val6"), None),
    Clause("EQ", Some("fields7"), Some("val7"), None),
  )

  import scala.concurrent.Await
  import scala.concurrent.duration._

  private val db: Database = Database.forConfig("configtest")

  def exec[T](dBIOAction: DBIO[T]): T = Await.result(db.run(dBIOAction), 2 second)

  def main(args: Array[String]): Unit = {
    //    inserterGeneric(clause1, "1 part clause")
    //    inserterGeneric(clause3, "3 part clause")
    //    inserterGeneric(clause5, "5 part clause")
    //    inserterGeneric(clause7, "7 part clause")
    inserterGeneric(clause13, "13 part clause")
  }

  def inserterGeneric(cs: Seq[Clause], msg: String): Unit = {
    logger.debug(s"$msg")
    exec(
      ddl.create >>
        insertClauses(cs)
    )
    exec(clauses.result).foreach(r => logger.debug(s"$r"))
  }

  def debug(acc: List[ACC]) = acc foreach (s => logger.debug(s"$s"))
}
