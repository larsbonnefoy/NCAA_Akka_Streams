package NcaaPipeFilter

import java.time.LocalDate
/**
  * Class representing a Row from the input CSV file
  */
case class CsvRow(
  season: Int,
  round: Int,
  day: String,
  winTeam: String,
  loseTeam: String,
  winPoints: Int,
  losePoints: Int
  )

case class SundayVictories(team: String, numberWins: Int) {
  override def toString: String = s"Name: ${team} --> Won Games on Sundays: ${numberWins}"
}

case class PointsVictories(team: String, numberWins: Int) {
  override def toString: String = s"Name: ${team} --> Won ${numberWins} games by more than 5 points"
}

case class RoundInstances(team: String, number: Int) {
  override def toString: String = s"Name: ${team} --> Was ${number} times in quarter finals"
}

case class YearlyLosses(team: String, numberLosses: Int) {
  override def toString: String = s"Name: ${team} --> Lost ${numberLosses} times between 1980 and 1990"
}
