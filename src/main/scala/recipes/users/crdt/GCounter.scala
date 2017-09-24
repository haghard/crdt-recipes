package recipes.users.crdt

import scala.language.higherKinds
import cats.Monoid
import cats.syntax.semigroup._
import cats.syntax.foldable._


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

/*
  increment requires a monoid;
  total requires a commutative monoid
  merge required an idempotent commutative monoid, also called a bounded semilattice.
 */

trait GCounter[F[_,_],K, V] {
  /*
   * Needs an identity to initialise the counter. We also rely on associativity to ensure the specific sequence
   * of additions we perform gives the correct value.
   * For example :
   *  increment(Map("a" -> 7, "b" -> 3))("a", 8) -> Map("a" -> 8, "b" -> 3) because 8 > 7
   *  increment(Map("a" -> 7, "b" -> 3))("a", 1) -> Map("a" -> 7, "b" -> 3)
   */
  def increment(f: F[K, V])(k: K, v: V)(implicit m: Monoid[V]): F[K, V]
  /*
   * Implicitly relies on associativity and commutivity to ensure we get the correct value no matter
   * what arbitrary order we choose to sum the per-machine counters. We also implicitly assume an identity,
   * which allows us to skip machines for which we do not store a counter.
   */
  def get(f: F[K, V])(implicit m: Monoid[V]): V
  /*
    Requires a bounded semilattice (or idempotent commutative monoid).
    We rely on commutivity to ensure that machine A merging with machine B yields the same result
    as machine B merging with machine A.
    We need associativity to ensure we obtain the correct result when three or more machines are merging data.
    We need an identity element to initialise empty counters.
    Finally, we need an additional property, called idempotency, to ensure that if two machines hold the same data
    in a per-machine counter, merging data will not lead to an incorrect result.
  */
  def merge(f1: F[K, V], f2: F[K, V])(implicit b: BoundedSemiLattice[V]): F[K, V]
}

object GCounter {
  implicit def mapGCounter[K, V]: GCounter[Map, K, V] =
    new GCounter[Map, K, V] {
      import cats.instances.map._

      def increment(fa: Map[K, V])(k: K, v: V)(implicit m: Monoid[V]): Map[K, V] =
        fa + (k -> (fa.getOrElse(k, m.empty) |+| v))

      def get(f: Map[K, V])(implicit m: Monoid[V]): V =
        f.foldMap(identity)

      def merge(f1: Map[K, V], f2: Map[K, V])(implicit b: BoundedSemiLattice[V]): Map[K, V] =
        f1 |+| f2
    }

  def apply[F[_,_],K, V](implicit g: GCounter[F, K, V]) = g
}

/*
val g1 = Map("a" -> 7, "b" -> 3)
val g2 = Map("a" -> 2, "b" -> 5)

GCounter[Map, String, Int].merge(g1, g2)
GCounter[Map, String, Int].get(g1)
*/

