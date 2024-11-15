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

enum Question:
    case SundayVictories, PointsVictories, QuarterTimes, YearlyLosses

case class Answer(qType: Question, team: String, cntr: Int) {
    override def toString: String = {
      qType match {
        case Question.SundayVictories => s"Name: ${team} --> Won Games on Sundays: ${cntr}"
        case Question.PointsVictories => s"Name: ${team} --> Won Games with more than 5 pts: ${cntr}"
        case Question.QuarterTimes => s"Name: ${team} --> Times in quarters: ${cntr}"
        case Question.YearlyLosses => s"Name: ${team} --> Lost games between 1980 and 1990: ${cntr}"
      }
    }
}
