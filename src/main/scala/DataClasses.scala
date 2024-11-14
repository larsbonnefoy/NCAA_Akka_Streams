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
    case SundayVictories, PointsVictories, RoundInstances, YearlyLosses

case class Answer(qType: Question, team: String, cntr: Int) {
    override def toString: String = {
      qType match {
        case Question.SundayVictories => s"Name: ${team} --> Won Games on Sundays: ${cntr}"
        case _ => s"(${qType}, ${team}, ${cntr})"
      }
    }
}
