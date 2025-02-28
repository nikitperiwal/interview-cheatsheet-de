"""
A and B are playing the famous Ball Score Game. In this game, there are some balls placed on a table.
Each ball has a value written on it. The game starts with a coin toss.

If the coin falls heads side up, then A starts the game, else B starts the game. The game is played in alternative turns.
The player who wins the toss takes the first turn. During each turn, a player is allowed to pick at most K balls
from the table. A score of a player is the sum of values of all the balls taken by the player.

A doesn't care about the sum of digits so he can pick any ball from the table.
B is crazy about the sum of digits thus he will only pick the ball whose sum of digits of the ball value is maximum
 (for e.g. if 4 and 11 are present on the table, he will pick 4 as the sum of digits of 4 is greater than that of 11).
  If more than one ball has the maximum sum of digits, then he can pick any one of them.

They both want to maximize their score and so both of them will play optimally. Print the score A and B will achieve.

Input 1:
K = 2
Balls = 1000, 99, 98
Toss outcome = TAILS

Output 1:
Score of A = 1000
Score of B = 197

Input 2:
K=1
Balls = 5, 6
Toss outcome = HEADS

Output 2:
Score of A = 6
Score of B = 5

"""