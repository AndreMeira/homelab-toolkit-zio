package homelab.incubator.common.data.v5


import zio.*
import zio.test.*


/**
 * A real batched-transaction use-case exercised against a fake repository, to validate that the pure
 * `Batch` surface composes cleanly with an effectful bulk workflow. The `Batch` juggling between the two
 * bulk effects is pure — in production the whole for-comprehension body runs inside a `db.transaction { … }`.
 */
object BulkGrantSpec extends ZIOSpecDefault:

  final case class Account(id: String, balance: Int, version: Int)
  final case class Grant(accountId: String, amount: Int)

  /** Fake account store — stands in for a repository running inside a transaction. */
  final class AccountRepo(state: Ref[Map[String, Account]]):
    def findAll(ids: List[String]): UIO[List[Account]]      =
      state.get.map(accounts => ids.flatMap(accounts.get))
    def saveAll(updates: List[Account]): UIO[List[Account]] =
      state.modify { accounts =>
        val persisted = updates.collect { case a if accounts.contains(a.id) => a.copy(version = a.version + 1) }
        (persisted, accounts ++ persisted.map(a => a.id -> a))
      }

  /** Credit a bonus to each granted account, one round-trip per step, returning a per-grant result in order. */
  def grantBonuses(repo: AccountRepo, grants: List[Grant]): UIO[List[Either[String, Account]]] =
    val batch = Batch.make(grants)
    for
      // 1. bulk-read the targeted accounts
      found   <- repo.findAll(batch.success.toList.map(_.accountId))
      accounts = batch.success.indexBy(_.accountId).associateWith(found, "no such account")(_.id)
      // 2. credit the accounts that exist, and bulk-write them (the store returns the persisted rows)
      toSave = accounts.map((grant, account) => account.copy(balance = account.balance + grant.amount))
      saved   <- repo.saveAll(toSave.success.toList)
    // 3. left-join the persisted rows back onto the grants by account id — a grant with no saved row is an error
    yield batch.success.indexBy(_.accountId).replaceWith(saved, "no such account")(_.id).toList

  def spec = suite("Batch v5 — batched transaction use-case")(
    test("credits found accounts, reports missing ones, in request order") {
      val initial = Map("a" -> Account("a", 100, 0), "b" -> Account("b", 50, 0))
      val grants  = List(Grant("a", 10), Grant("c", 5), Grant("b", 20))
      for
        state  <- Ref.make(initial)
        repo    = new AccountRepo(state)
        result <- grantBonuses(repo, grants)
        finalA <- state.get.map(_("a"))
      yield assertTrue(
        result == List(
          Right(Account("a", 110, 1)),
          Left("no such account"),
          Right(Account("b", 70, 1)),
        ),
        finalA == Account("a", 110, 1), // the write actually landed
      )
    }
  )
