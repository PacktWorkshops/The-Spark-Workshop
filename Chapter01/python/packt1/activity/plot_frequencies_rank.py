from plotly.offline import plot
from plotly.graph_objs import Figure, Scatter, Marker, Line
import plotly.graph_objs as go


all_ranks = []
frequent_ranks = []  # for elements occurring at least twice
frequencies = []
rankfrequ = []
words = []

rank = 1

with open('frequencies.dat', "r") as ins:
    for line in ins:  # (2190,the)
        line = line.strip()
        pair = line.split(',')
        frequency = int(pair[0][1:])  # remove (
        word = pair[1][0:-1]  # remove )
        all_ranks.append(rank)
        frequencies.append(frequency)
        if frequency > 1:
            frequent_ranks.append(rank)
            rank_frequ = rank * frequency
            rankfrequ.append(rank_frequ)
        words.append(word)
        rank += 1


rank_frequ = go.Scatter(
    x=all_ranks,
    y=frequencies,
    mode='lines+markers',
    name='rank_frequencies',
    text=words,
    textposition='top center',
)

rank_frequ2 = go.Scatter(
    x=frequent_ranks,
    y=rankfrequ,
    mode='lines+markers',
    name='rank_frequencies',
    text=words,
    textposition='top center',
)

rank_frequ_data2 = [rank_frequ2]
# rank_frequ_data2 = [rank_frequ, rank_frequ2]
fig2 = Figure(data=rank_frequ_data2)
plot(fig2, filename='rankfrequ_plot.html')