package NcaaPipeFilter

import java.time.LocalDate
/**
  * Class representing a Row from the input CSV file
  */
case class CsvRow(
  season: String,
  round: Int,
  daysFromEpoch: String,
  gameDate: LocalDate,
  day: String,
  winTeam: String,
  loseTeam: String,
  winPoints: Int,
  losePoints: Int
  //winSeed: Int,
  //loseSeed: Int,
  //numOt: Int,
  //academicYear: String
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
