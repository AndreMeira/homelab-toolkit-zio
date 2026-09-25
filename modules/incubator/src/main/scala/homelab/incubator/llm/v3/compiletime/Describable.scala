package homelab.incubator.llm.v3.compiletime

import scala.quoted.*


/**
 * Refusing an undescribable type while the compiler is still running.
 *
 * A sketch, to find out how far compile-time checking can go. `Generator.generate` answers at runtime
 * because it walks a `zio.schema.Schema[A]`, and that is a value built *by* generated code rather than
 * something the typer can see. A macro cannot read it — but it can read the type, which is enough to catch
 * the shapes that are wrong whatever schema they are given.
 *
 * What it catches is therefore a subset, and deliberately the uncontroversial one: a `Map`, a tuple, or
 * `Unit` anywhere in the arguments of a tool. What it cannot catch is anything that depends on which
 * `Schema` instance is in scope — see [[check]].
 */
object Describable {

  /**
   * Refuse `A` at compile time when it is plainly undescribable.
   *
   * Walks the type: a case class through its fields, a sealed hierarchy through its children, and any type
   * through its arguments. A `Map`, a tuple or `Unit` met anywhere aborts compilation, naming the path that
   * reached it.
   *
   * Silence is not a proof. A type can pass here and still be refused by the derivation, because a schema
   * decides things a type does not: a given `Schema[Map[K, V]]` may render as an association list, a
   * `Schema.Transform` may stand for something else entirely, and a sum type carrying data is refused
   * unless annotated. This says "not obviously wrong", never "describable".
   *
   * @tparam A the type a tool would take as its arguments
   */
  inline def check[A]: Unit = ${ checkExpr[A] }

  /**
   * The macro behind [[check]].
   *
   * @tparam A the type to walk
   * @return the unit expression the call site becomes, once the walk has found nothing
   */
  private def checkExpr[A: Type](using Quotes): Expr[Unit] =
    import quotes.reflect.*

    def refuse(what: String, path: List[String]): Nothing =
      report.errorAndAbort(s"$what cannot be described to a model, at ${path.reverse.mkString(" → ")}")

    def walk(repr: TypeRepr, path: List[String], seen: Set[Symbol]): Unit =
      val widened = repr.widen.dealias
      val symbol  = widened.typeSymbol
      val name    = symbol.name

      if name == "Map" || name.startsWith("Tuple") && name != "Tuple$package" then refuse(s"a $name", path)
      else if widened =:= TypeRepr.of[Unit] then refuse("Unit", path)
      else if seen.contains(symbol) then () // a recursive knot: walked already
      else
        val next = seen + symbol
        widened match
          case applied: AppliedType => applied.args.foreach(arg => walk(arg, path, next))
          case _                    => ()

        if symbol.flags.is(Flags.Case) && symbol.isClassDef then
          symbol.caseFields.foreach: field =>
            walk(widened.memberType(field), s"${symbol.name}.${field.name}" :: path, next)
        else if symbol.flags.is(Flags.Sealed) && symbol.isClassDef then
          symbol.children.foreach: child =>
            walk(child.typeRef, s"${symbol.name}.${child.name}" :: path, next)

    walk(TypeRepr.of[A], List(Type.show[A]), Set.empty)
    '{ () }
}
