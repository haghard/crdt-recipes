package recipes.users.crdt

object GCounterExample {
  import cats.{Monoid, Foldable}
  import cats.syntax.foldable._
  import cats.syntax.semigroup._

  trait BoundedSemiLattice[A] extends Monoid[A] {
    def combine(a1: A, a2: A): A
    def empty: A
  }

  object BoundedSemiLattice {
    implicit object intBoundedSemiLatticeInstance extends BoundedSemiLattice[Int] {
      override def combine(a1: Int, a2: Int): Int = a1 max a2
      override val empty: Int = 0
    }

    implicit def setBoundedSemiLatticeInstance[A]: BoundedSemiLattice[Set[A]] =
      new BoundedSemiLattice[Set[A]] {
        override def combine(a1: Set[A], a2: Set[A]): Set[A] = a1 union a2
        override val empty: Set[A] = Set.empty[A]
      }
  }

  trait GCounter[F[_, _], K, V] {
    def increment(f: F[K, V])(key: K, value: V)(implicit m: Monoid[V]): F[K, V]
    def total(f: F[K, V])(implicit m: Monoid[V]): V
    def merge(f1: F[K, V], f2: F[K, V])(implicit b: BoundedSemiLattice[V]): F[K, V]
  }

  object GCounter {
    def apply[F[_, _], K, V](implicit g: GCounter[F, K, V]) = g

    implicit class GCounterOps[F[_, _], K, V](f: F[K, V]) {
      def increment(key: K, value: V)(implicit g: GCounter[F, K, V], m: Monoid[V]): F[K, V] =
        g.increment(f)(key, value)

      def total(implicit g: GCounter[F, K, V], m: Monoid[V]): V =
        g.total(f)

      def merge(that: F[K, V])(implicit g: GCounter[F, K, V], b: BoundedSemiLattice[V]): F[K, V] =
        g.merge(f, that)
    }

    implicit def keyValueInstance[F[_, _], K, V](
      implicit k: KeyValueStore[F], km: Monoid[F[K, V]],
      kf: Foldable[({type l[A] = F[K, A]})#l]): GCounter[F, K, V] =
      new GCounter[F, K, V] {
        import cats.instances.map._
        import KeyValueStore._ // For KeyValueStore syntax

        def increment(f: F[K, V])(key: K, value: V)(implicit m: Monoid[V]): F[K, V] =
          f + (key, (f.getOrElse(key, m.empty) |+| value))

        def total(f: F[K, V])(implicit m: Monoid[V]): V =
          f.foldMap(identity _)

        def merge(f1: F[K, V], f2: F[K, V])(implicit b: BoundedSemiLattice[V]): F[K, V] =
          f1 |+| f2
      }
  }

  trait KeyValueStore[F[_, _]] {
    def +[K, V](f: F[K, V])(key: K, value: V): F[K, V]
    def get[K, V](f: F[K, V])(key: K): Option[V]
    def getOrElse[K, V](f: F[K, V])(key: K, default: V): V =
      get(f)(key).getOrElse(default)
  }

  object KeyValueStore {
    implicit class KeyValueStoreOps[F[_, _], K, V](f: F[K, V]) {
      def +(key: K, value: V)(implicit kv: KeyValueStore[F]): F[K, V] =
        kv.+(f)(key, value)

      def get(key: K)(implicit kv: KeyValueStore[F]): Option[V] =
        kv.get(f)(key)

      def getOrElse(key: K, default: V)(implicit kv: KeyValueStore[F]): V =
        kv.getOrElse(f)(key, default)
    }

    implicit object mapKeyValueStoreInstance extends KeyValueStore[Map] {
      def +[K, V](f: Map[K, V])(key: K, value: V): Map[K, V] =
        f + (key, value)

      def get[K, V](f: Map[K, V])(key: K): Option[V] =
        f.get(key)

      override def getOrElse[K, V](f: Map[K, V])(key: K, default: V): V =
        f.getOrElse(key, default)
    }
  }
}


object Example {

  import cats.instances.map._
  import cats.instances.int._

  import GCounterExample.KeyValueStore._
  import GCounterExample.GCounter._

  val crdt1 = Map("a" -> 1, "b" -> 3, "c" -> 5)
  val crdt2 = Map("a" -> 2, "b" -> 4, "c" -> 6)

  crdt1.increment("a", 20).merge(crdt2).total
}














       





