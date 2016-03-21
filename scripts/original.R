#  MultiProcessing : True   MaximumTravelers : 5000	 Interval : 0.2	 Timer: 0:20:00	 Store: PythonRemoteStore
library(Hmisc)
library(TTR)
#setwd("/home/arthur/Dropbox/workspace/mobdat/stats")
setwd("/Users/arthurvaladares/Dropbox/workspace/mobdat/stats")
osfn = "2016-03-17_17-20-17_original_OpenSimConnector.csv"
socialfn = "2016-03-17_17-20-18_original_SocialConnector.csv"
sumofn = "2016-03-17_17-20-18_original_SumoConnector.csv"

os <- read.csv(file=osfn, header=TRUE, sep=",", fileEncoding="UTF-8", na.strings='NULL', skip=3)
sumo <- read.csv(file=sumofn, header=TRUE, sep=",", fileEncoding="UTF-8", na.strings='NULL', skip=3)
social <- read.csv(file=socialfn, header=TRUE, sep=",", fileEncoding="UTF-8", na.strings='NULL', skip=3)

os$convtime = strptime(os$time, "%M:%OS")
sumo$convtime = strptime(sumo$time, "%H:%M:%OS")
social$convtime = strptime(social$time, "%H:%M:%OS")

#os = subset(os, HandleEvent < 500)
#sumo = subset(sumo, HandleEvent < 500)
#social = subset(social, HandleEvent < 500)

plot(os$step, os$vehicles, col='white', ylim=c(0,1200))
#lines(os$convtime, os$vehicles, col='blue')
#lines(sumo$convtime, sumo$vehicles, col='green')
#lines(social$convtime, social$vehicles, col='orange')
lines(os$step, os$vehicles, col='blue')
lines(sumo$step, sumo$vehicles, col='green')
lines(social$step, social$vehicles, col='orange')

plot(os$convtime, os$HandleEvent, col='white', ylim=c(0,230))

# moving average
los = EMA(os$HandleEvent, 20)
lsumo = EMA(sumo$HandleEvent, 20)
lsocial = EMA(social$HandleEvent, 20)

lines(os$convtime,los, col='blue')
lines(sumo$convtime,lsumo, col='green')
lines(social$convtime,lsocial, col='orange')

length(sumo$convtime)

legend('topright', legend=c('opensim','sumo','social'), col=c('blue','green','orange'), lty=1)
