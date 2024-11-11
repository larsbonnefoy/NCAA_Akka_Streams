package NcaaPipeFilter

import java.time.LocalDate
/**
  * Class representing a Row from the input CSV file
  */
case class CsvRow(
  season: String,
  round: String,
  daysFromEpoch: String,
  gameDate: LocalDate,
  day: String,
  winTeam: String, //just alias atm
  loseTeam: String, //just alias atm
  //winSeed: Int,
  //loseSeed: Int,
  //numOt: Int,
  //academicYear: String
  )

case class SundayVictories(
  team: String,
  numberWins: Int,
)
