#import libraries for data visualization
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, SQLContext, Row

spark = SparkSession.builder.appName("kdd").getOrCreate()
sc = spark.sparkContext

# read data from google cloud storage
data_file = "gs://premier_league_season_2000to2022/EPL Standings 2000-2022.jsonl"
data = spark.read.json(data_file).cache()

# change data type to Pandas DataFrame
pdf = data.toPandas()

# change data types to numeric
pdf['W'] = pd.to_numeric(pdf['W'])
pdf['Pld'] = pd.to_numeric(pdf['Pld'])
pdf['D'] = pd.to_numeric(pdf['D'])
pdf['L'] = pd.to_numeric(pdf['L'])
pdf['GF'] = pd.to_numeric(pdf['GF'])
pdf['Pos'] = pd.to_numeric(pdf['Pos'])
pdf['Pts'] = pd.to_numeric(pdf['Pts'])

Seasons = pdf['Season'].values

pos = pdf.loc[pdf['Team'] == "Manchester City", 'Pos'].values

# Plot the ManCity League Positions over the years
plt.plot(Seasons, pos)

# Add labels and title to the plot
plt.xlabel('Season')
plt.ylabel('Position')
plt.title('ManCity Positions')

# Display the plot
plt.show()

######################################################################################################################

goals = pdf.loc[pdf['Team'] == "Manchester City", 'GF'].values

# Plot the ManCity League goals over the years
plt.plot(Seasons, goals)

# Add labels and title to the plot
plt.xlabel('Year')
plt.ylabel('Goals For')
plt.title('')

# Display the plot
plt.show()

######################################################################################################################

man_city = pdf[pdf['Team'] == 'Manchester City']

# Plot the ManCity Points scored over the years
man_city.plot(x='Season', y='Pts', marker = 'o', ms = '6', lw = '3', mfc = 'black', c = 'red')

plt.xlabel('Season')
plt.ylabel('Points')

plt.title("Manchester City Points")
plt.legend(['Pts']);
plt.gca().invert_yaxis()

######################################################################################################################

# Getting Manchester City's Wins, Loses, and Draws from 2000-2007
d = pdf[(pdf['Season'].between('2000-01', '2007-08'))]

wins = d.loc[(d["Team"] == "Manchester City"), 'W'].sum()
loses = d.loc[(d["Team"] == "Manchester City"), 'L'].sum()
draws = d.loc[(d["Team"] == "Manchester City"), 'D'].sum()

values = [wins, loses, draws]
labels = ['Wins', 'Loses', 'Draws']

# Create the pie chart
fig, ax = plt.subplots()
ax.pie(values, labels=labels, autopct='%1.1f%%', startangle=90)

# Add a title to the chart
plt.title('Seasons 2008-2021')

# Display the chart
plt.show()

# Getting Manchester City's Wins, Loses, and Draws from 2008-2021
d = pdf[(pdf['Season'].between('2008-09', '2021-22'))]

wins = pdf.loc[(pdf["Team"] == "Manchester City"), 'W'].sum()
loses = pdf.loc[(pdf["Team"] == "Manchester City"), 'L'].sum()
draws = pdf.loc[(pdf["Team"] == "Manchester City"), 'D'].sum()

values = [wins, loses, draws]
labels = ['Wins', 'Loses', 'Draws']

# Create the pie chart
fig, ax = plt.subplots()
ax.pie(values, labels=labels, autopct='%1.1f%%', startangle=90)

# Add a title to the chart
plt.title('Wins, Loses, and Draws')

# Display the chart
plt.show()

######################################################################################################################

# Get the second rows for each season
second_rows = pdf.groupby("Season").nth(1)

# Get the rows where Manchester City is in position 1
city_pos1 = pdf[(pdf["Team"] == "Manchester City") & (pdf["Pos"] == 1)]

# Merge the second rows with the rows where Manchester City is in position 1
second_rows_pos1 = pd.merge(second_rows, city_pos1[["Season"]], on="Season", how="inner")

man_city = pdf.loc[(pdf['Team'] == "Manchester City") & (pdf['Pos'] == 1)]
man_city_pts = man_city["Pts"].values

##################################################

# Plot the ManCity Points difference between it when it wins the League title and its runner-up
colors = np.array(['blue', 'orange'])
fig, ax = plt.subplots()
bars1 = ax.barh(labels, man_city_pts, color=colors[0], height=0.5, align='center', label='Values 1')
bars2 = ax.barh(labels, -runner_up_pts, color=colors[1], height=0.5, align='center', label='Values 2')
ax.axvline(x=0, color='black', linewidth=0.5)
ax.set_xlim([-105, 105])
ax.set_xlabel('Points')
ax.set_ylabel('Season')
ax.set_title('Manchester City and Runner-up Points')
plt.show()

######################################################################################################################

# Plot the ManCity Points difference between it when it wins the League title and its runner-up
width = 0.35

fig, ax = plt.subplots()
ax.bar(np.arange(len(manu_pts))-width/2, manu_pts, width, label='Manchester United')
ax.bar(np.arange(len(mancity_pts))+width/2, mancity_pts, width, label='Manchester City')

plt.title('Manchester City and Runner-up Points')
plt.xlabel('')
plt.ylabel('Points')
plt.xticks(np.arange(len(labels)), labels)

plt.show()

######################################################################################################################

# Define number of times each of the following teams won the League title from 2000-2007
Man_Uited = pdf.loc[(pdf['Team'] == "Manchester United") & (pdf['Pos'] == 1) & (pdf['Season'].between('2000-01', '2007-08')), 'Pos'].count()

Man_City = pdf.loc[(pdf['Team'] == "Manchester City") & (pdf['Pos'] == 1) & (pdf['Season'].between('2000-01', '2007-08')), 'Pos'].count()

Chelsea = pdf.loc[(pdf['Team'] == "Chelsea") & (pdf['Pos'] == 1) & (pdf['Season'].between('2000-01', '2007-08')), 'Pos'].count()

Arsenal = pdf.loc[(pdf['Team'] == "Arsenal") & (pdf['Pos'] == 1) & (pdf['Season'].between('2000-01', '2007-08')), 'Pos'].count()

Livepool = pdf.loc[(pdf['Team'] == "Liverpool") & (pdf['Pos'] == 1) & (pdf['Season'].between('2000-01', '2007-08')), 'Pos'].count()

values = [Man_Uited, Man_City, Chelsea, Arsenal, Livepool]
labels = ['Man_United', 'Man_City', 'Chelsea', 'Arsenal', 'Livepool']

# Plot the Pie chart
fig, ax = plt.subplots()
ax.pie(values, labels=labels, autopct='%1.1f%%', startangle=90)

# Add a title to the chart
plt.title('League Titles Winners')

# Display the chart
plt.show()

###########################################################

# Define number of times each of the following teams won the League title from 2008-2021
Man_Uited = pdf.loc[(pdf['Team'] == "Manchester United") & (pdf['Pos'] == 1) & (pdf['Season'].between('2008-09', '2021-22')), 'Pos'].count()

Man_City = pdf.loc[(pdf['Team'] == "Manchester City") & (pdf['Pos'] == 1) & (pdf['Season'].between('2008-09', '2021-22')), 'Pos'].count()

Chelsea = pdf.loc[(pdf['Team'] == "Chelsea") & (pdf['Pos'] == 1) & (pdf['Season'].between('2008-09', '2021-22')), 'Pos'].count()

Arsenal = pdf.loc[(pdf['Team'] == "Arsenal") & (pdf['Pos'] == 1) & (pdf['Season'].between('2008-09', '2021-22')), 'Pos'].count()

Livepool = pdf.loc[(pdf['Team'] == "Liverpool") & (pdf['Pos'] == 1) & (pdf['Season'].between('2008-09', '2021-22')), 'Pos'].count()

values = [Man_Uited, Man_City, Chelsea, Arsenal, Livepool]
labels = ['Man_United', 'Man_City', 'Chelsea', 'Arsenal', 'Livepool']

# Create the pie chart
fig, ax = plt.subplots()
ax.pie(values, labels=labels, autopct='%1.1f%%', startangle=90)

# Add a title to the chart
plt.title('League Titles Winners')

# Display the chart
plt.show()

######################################################################################################################

# Get the Goals Scored and conceded for Manchester City from 2000-2007
man_city = pdf[(pdf["Team"] == "Manchester City") & (pdf['Season'].between('2000-01', '2007-08'))]

fig, ax = plt.subplots()

# plot the bars for each metric
ax.bar(man_city['Season'], man_city['GF'], label='Goals Scored')
ax.bar(man_city['Season'], man_city['GA'], bottom=man_city['GF'], label='Goals Against')

plt.xticks(rotation=75)

# set labels and title
ax.set_xlabel('Season')
ax.set_ylabel('')
ax.set_title('GF vs GA')

# add legend
ax.legend()
plt.show()

###########################################################

# Get the Goals Scored and conceded for Manchester City from 2008-2021
man_city = pdf[(pdf["Team"] == "Manchester City") & (pdf['Season'].between('2008-09', '2021-22'))]

fig, ax = plt.subplots()

# plot the bars for each metric
ax.bar(man_city['Season'], man_city['GF'], label='Goals Scored')
ax.bar(man_city['Season'], man_city['GA'], bottom=man_city['GF'], label='Goals Against')
#ax.bar(man_city['Season'], man_city['GD'], bottom=man_city['GF'] + man_city['GA'], label='Goals Difference')

plt.xticks(rotation=75)

# set labels and title
ax.set_xlabel('Season')
ax.set_ylabel('')
ax.set_title('GF vs GA')

# add legend
ax.legend()

plt.show()

######################################################################################################################

# Get the Points scored by Manchester City and Manchester United to compare them
manu_pts = pdf.loc[(pdf['Team'] == 'Manchester United') & (pdf['Pos'] == 1), 'Pts'].values
mancity_pts = pdf.loc[(pdf['Team'] == 'Manchester City') & (pdf['Pos'] == 1), 'Pts'].values

width = 0.35

# Plot the bar chart
fig, ax = plt.subplots()
ax.bar(np.arange(len(manu_pts))-width/2, manu_pts, width, label='Manchester United')
ax.bar(np.arange(len(mancity_pts))+width/2, mancity_pts, width, label='Manchester City')

plt.title('Points scored by Manchester United and Manchester City')
plt.xlabel('')
plt.ylabel('Points')
plt.xticks(np.arange(len(Seasons)), Seasons)
plt.legend()
plt.show()

######################################################################################################################

# Get the Points scored by Manchester City and Manchester United to compare them
manu_goals = United_pts = pdf.loc[(pdf['Team'] == 'Manchester United') & (pdf['Pos'] == 1), 'GF'].values
mancity_goals = pdf.loc[(pdf['Team'] == 'Manchester City') & (pdf['Pos'] == 1), 'GF'].values

labels = ['1', '2', '3', '4', '5', '6']
width = 0.35

fig, ax = plt.subplots()
ax.bar(np.arange(len(manu_goals))-width/2, manu_goals, width, label='Manchester United')
ax.bar(np.arange(len(mancity_goals))+width/2, mancity_goals, width, label='Manchester City')

plt.title('Points scored by Manchester United and Manchester City')
plt.xlabel('')
plt.ylabel('Goals Scored')
plt.xticks(np.arange(len(labels)), labels)
plt.legend()
plt.show()

######################################################################################################################

# Getting the overall Performance of the TOP 6 teams
man_utd_df = pdf[pdf['Team'] == 'Manchester United']
man_utd_df = man_utd_df['Pos'].values

man_cty_df = pdf[pdf['Team'] == 'Manchester City']
man_cty_df = man_cty_df['Pos'].values
man_cty_df = np.append(man_cty_df, 20)

arsenal = pdf[pdf['Team'] == 'Arsenal']
arsenal = arsenal['Pos'].values

chelsea = pdf[pdf['Team'] == 'Chelsea']
chelsea = chelsea['Pos'].values

liverpool = pdf[pdf['Team'] == 'Liverpool']
liverpool = liverpool['Pos'].values

tot = pdf[pdf['Team'] == 'Tottenham Hotspur']
tot = tot['Pos'].values

for i in range(len(22)):
    man_utd_df[i] = 21 - man_utd_df[i]
    man_cty_df[i] = 21 - man_cty_df[i]
    arsenal[i] = 21 - arsenal[i]
    chelsea[i] = 21 - chelsea[i]
    liverpool[i] = 21 - liverpool[i]
    tot[i] = 21 - tot[i]

x = ['United', 'City', 'Arsenal' , 'Chelsea' , 'Liverpool',  'Tottenham']
y = [np.mean(man_utd_df), np.mean(man_cty_df), np.mean(arsenal), np.mean(chelsea), np.mean(liverpool), np.mean(tot)]

# create bar chart
fig, ax = plt.subplots()
ax.bar(x, y)
ax.set_xlabel('Team')
ax.set_ylabel('Average Performance')
ax.set_title('Comparison of Average Performance of the TOP 6')
# show the plot
plt.show()