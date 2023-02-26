import json
import random
import datetime


minPlayerId = 100
maxPlayerId = 110
minGameId = 10
maxGameId = 15
minWin = 100
maxWin = 500
minWinIndicator = 1
maxWinIndicator = 12
winModulu = 3
minStake = 10
maxStake = 300


def generateGamePlay() :
  win = 0
  playerId = random.randint(minPlayerId, maxPlayerId)
  gameId = random.randint(minGameId, maxGameId)
  stake = random.randint(minStake, maxStake)
  eventTime = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
  if((random.randint(minWinIndicator, maxWinIndicator) %  winModulu) == 0 ):
    win = random.randint(minWin, maxWin)


  retVal = {
    "playerId": playerId,
    "gameId": gameId,
    "stake": stake,
    "win": win,
    "eventTime": eventTime
  }

  return playerId, json.dumps(retVal)




