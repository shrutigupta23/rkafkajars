/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2006-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scala.util.parsing.input

import collection.mutable.ArrayBuffer

/** <p>
 *    <code>OffsetPosition</code> is a standard class for positions
 *    represented as offsets into a source ``document''.
 *    @param source   The source document
 *    @param offset   The offset indicating the position
 *
 * @author Martin Odersky
 */
case class OffsetPosition(source: java.lang.CharSequence, offset: Int) extends Position {

  /** An index that contains all line starts, including first line, and eof */
  private lazy val index: Array[Int] = {
    var lineStarts = new ArrayBuffer[Int]
    lineStarts += 0
    for (i <- 0 until source.length)
      if (source.charAt(i) == '\n') lineStarts += (i + 1)
    lineStarts += source.length
    lineStarts.toArray
  }

  /** The line number referred to by the position; line numbers start at 1 */
  def line: Int = {
    var lo = 0
    var hi = index.length - 1
    while (lo + 1 < hi) {
      val mid = (hi + lo) / 2
      if (offset < index(mid)) hi = mid
      else lo = mid
    } 
    lo + 1
  }
   
  /** The column number referred to by the position; column numbers start at 1 */
  def column: Int = offset - index(line - 1) + 1

  /** The contents of the line numbered `lnum' (must not contain a new-line character).
   * 
   * @param lnum a 1-based integer index into the `document'
   * @return the line at `lnum' (not including a newline)
   */
  def lineContents: String =
    source.subSequence(index(line - 1), index(line)).toString
  
  /** Returns a string representation of the `Position', of the form `line.column' */
  override def toString = line+"."+column

  /** Compare this position to another, by first comparing their line numbers,
   * and then -- if necessary -- using the columns to break a tie.
   *
   * @param `that' a `Position' to compare to this `Position'
   * @return true if this position's line or (in case of a tie wrt. line numbers)
   *         its column is smaller than the corresponding components of `that'
   */
  override def <(that: Position) = that match {
    case OffsetPosition(_, that_offset) =>
      this.offset < that_offset
    case _ =>
      this.line < that.line || 
      this.line == that.line && this.column < that.column
  }
}
