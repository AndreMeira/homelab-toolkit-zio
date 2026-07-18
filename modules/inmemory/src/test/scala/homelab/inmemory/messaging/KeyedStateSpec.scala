package homelab.inmemory.messaging


import homelab.inmemory.messaging.KeyedQueue.KeyedState
import zio.test.*


// Pure transition tests for KeyedQueue's keyed-scheduling core: every case is a plain value
// computation, no effects — the driver's concurrency (publish, wake, brackets) is tested where the
// driver lives (the KeyedQueue-backed Distributer, in InMemoryMessagingSpec).
object KeyedStateSpec extends ZIOSpecDefault:

  def spec = suite("KeyedQueue.KeyedState")(
    test("enqueue signals ready exactly on the no-backlog transition") {
      val (first, s1)  = KeyedState.empty[Int, String].enqueue(1, "a")
      val (second, s2) = s1.enqueue(1, "b")
      assertTrue(
        first,                              // no-backlog → backlog: publish
        !second,                            // already backlogged: already published
        s2.pending(1) == Vector("a", "b"),  // per-key FIFO order kept
      )
    },
    test("enqueue onto a running key does not signal ready") {
      val (_, s1)     = KeyedState.empty[Int, String].enqueue(1, "a")
      val (_, s2)     = s1.claim(1)                 // key 1 now running
      val (ready, s3) = s2.enqueue(1, "b")
      assertTrue(!ready, s3.running == Set(1))      // re-publish happens via release, not enqueue
    },
    test("claim takes the head value and marks the key running — FIFO per key") {
      val (_, s1)     = KeyedState.empty[Int, String].enqueue(1, "a")
      val (_, s2)     = s1.enqueue(1, "b")
      val (value, s3) = s2.claim(1)
      assertTrue(value == "a", s3.pending(1) == Vector("b"), s3.running == Set(1))
    },
    test("claiming a key's last value drops it from the backlog") {
      val (_, s1) = KeyedState.empty[Int, String].enqueue(1, "a")
      val (_, s2) = s1.claim(1)
      assertTrue(s2.pending.isEmpty, s2.running == Set(1))
    },
    test("release signals re-ready only while backlog remains") {
      val (_, s1)      = KeyedState.empty[Int, String].enqueue(1, "a")
      val (_, s2)      = s1.enqueue(1, "b")
      val (_, s3)      = s2.claim(1)
      val (again, s4)  = s3.release(1)              // "b" still queued → re-publish
      val (_, s5)      = s4.claim(1)
      val (done, s6)   = s5.release(1)              // drained → no re-publish
      assertTrue(again, !done, s6 == KeyedState.empty[Int, String])
    },
    test("a released, drained key signals ready again on its next enqueue") {
      val (_, s1)     = KeyedState.empty[Int, String].enqueue(1, "a")
      val (_, s2)     = s1.claim(1)
      val (_, s3)     = s2.release(1)
      val (ready, _)  = s3.enqueue(1, "b")
      assertTrue(ready)                             // a fresh claimable period → publish once more
    },
  )
