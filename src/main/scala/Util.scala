

object Util {

  def measureSpendingTime(numberOfInsert: Int)(executingBody: => Unit): TestResult = {
    val before = System.currentTimeMillis()
    executingBody
    val spendingTime = System.currentTimeMillis() - before
    new TestResult(spendingTime, numberOfInsert)
  }
}