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
    case BigLoss, PointsVictories, Top5, YearlyLosses

type Score = Int

sealed trait Count {
  def +(other: Count): Count
  override def toString: String
}


object Count {
  def simple(value: Int): Count = SimpleCount(value)
  def winLoss(isWin: Boolean, score: Int): Count = 
    if (isWin) WinLossCount(1, score, 1) else WinLossCount(0, score, 1)
}

case class SimpleCount(value: Int) extends Count {
  def +(other: Count): Count = other match {
    case SimpleCount(otherValue) => SimpleCount(value + otherValue)
    case _ => throw new IllegalArgumentException("Cannot add different count types")
  }
  
  override def toString: String = value.toString
}

case class WinLossCount(wins: Int, totalScore: Int, gamesPlayed: Int) extends Count {
  def +(other: Count): Count = other match {
    case WinLossCount(otherWins, otherScore, otherGames) => 
      WinLossCount(wins + otherWins, totalScore + otherScore, gamesPlayed + otherGames)
    case _ => throw new IllegalArgumentException("Cannot add different count types")
  }
  
  def averageScore: Double = if (gamesPlayed > 0) totalScore.toDouble / gamesPlayed else 0.0
  
  override def toString: String = f"${averageScore}%.2f (Wins: ${wins}, Total Played: ${gamesPlayed})"
}

case class Answer(qType: Question, team: String, cntr: Count) {
    override def toString: String = {
      qType match {
        case Question.BigLoss => s"Name: ${team} --> Lost Games with more than 85 points: ${cntr}"
        case Question.PointsVictories => s"Name: ${team} --> Won Games with less than or 5 pts: ${cntr}"
        case Question.Top5 => s"Name: ${team} --> Average Points / Game: ${cntr}"
        case Question.YearlyLosses => s"Name: ${team} --> Times lost between 1980-1990: ${cntr}"
      }
    }
}
