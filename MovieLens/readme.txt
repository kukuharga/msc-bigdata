{\rtf1\ansi\ansicpg1252\cocoartf1404\cocoasubrtf470
{\fonttbl\f0\fmodern\fcharset0 CourierNewPSMT;\f1\fnil\fcharset0 Menlo-Regular;}
{\colortbl;\red255\green255\blue255;}
\paperw11900\paperh16840\margl1440\margr1440\vieww15900\viewh8400\viewkind0
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0

\f0\fs24 \cf0 How to build the source :\
<Project Dir> ant clean dist\
\
How to run the jar file for :\
Part A : \
<Project Dir> \CocoaLigature0 hadoop jar dist/TweetRio.jar uk.ac.qmul.job.TweetHistogramJob input out\
\
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0
\cf0 \CocoaLigature1 Part B-1 : \
<Project Dir> \CocoaLigature0 hadoop jar dist/TweetRio.jar uk.ac.qmul.job.TweetHourlyJob input out\CocoaLigature1 \
\CocoaLigature0 \
\CocoaLigature1 Part B-2 : \
Option 1: \
<Project Dir>\CocoaLigature0 hadoop jar dist/TweetRio.jar uk.ac.qmul.job.TweetHashtagJob input out\
The command above will run the process using default value \'911\'92 as the busiest hour.\
\CocoaLigature1 Option 2: \
\CocoaLigature0 You might want to set different value of busiest hour using following command :\
\CocoaLigature1 <Project Dir>\CocoaLigature0 hadoop jar dist/TweetRio.jar uk.ac.qmul.job.TweetHashtagJob input out 5\
Above command will run the job given parameter 5 as the busiest hour. \
Valid hour parameter is between 0-23.\
\CocoaLigature1 \
\CocoaLigature0 Part C-1 :\
Option 1 :\
\CocoaLigature1 <Project Dir>\CocoaLigature0 hadoop jar dist/TweetRio.jar uk.ac.qmul.job.TweetAthleteMentionsJob2 input out\
\pard\tx560\tx1120\tx1680\tx2240\tx2800\tx3360\tx3920\tx4480\tx5040\tx5600\tx6160\tx6720\pardirnatural\partightenfactor0
\cf0 This command will run the job using default secondary data set file location : /data/medalistsrio.csv
\f1\fs28 \
\

\f0\fs24 Option 2 : \
You might want to change the secondary data file location using following command : 
\f1\fs28 \
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0

\f0\fs24 \cf0 \CocoaLigature1 <Project Dir>\CocoaLigature0 hadoop jar dist/TweetRio.jar uk.ac.qmul.job.TweetAthleteMentionsJob2 input /tmp/medalistsrio.csv out\
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0
\cf0 \CocoaLigature1 \
}