## #####################################
## Get data from the front page (news)
## and from the back page (newest).
## Compare them: if same stories are on
## the front page and back page they are
## considered as HOT and ON TIME
## #####################################

import re
import time 
import webapp2
from operator import itemgetter
from google.appengine.ext import db
from google.appengine.api import urlfetch

## ====================================
## == Hot news storage, format design
## == so that custom denoising is possible
## ====================================

class HNNOISE(db.Model):
  etime = db.IntegerProperty()
  stories_score = db.FloatProperty()
  stories_n = db.IntegerProperty()
  stories_json = db.StringProperty()

## ====================================
## == Do the magic (ideally we would get
## == also an email if somethings wrong
## == but this is so simple extraction
## == that we don't anticipate major errors)
## ====================================

class ETLPage(webapp2.RequestHandler):
  def get(self):
## ---------------------------
## -- ETL Source 1: 
## -- Newest stories (back page)
    debug_info = "-----------------------\n" 
    data_newest = []
    result = urlfetch.fetch(url='https://news.ycombinator.com/newest',deadline=60)
    if result.status_code == 200:
      txt_data = result.content
      p = re.compile("<td class=.title.+?><a href=\"(.+?)\".*?>(.+?)<\/a>.+?((\d+) points?</span>|(\d+) \w+ ago</td>).+?<a href=.item\?id=(\d+).>")
      for m in p.finditer(txt_data):
	if m.group(4):
	  data_newest.append([int(m.group(4)),int(m.group(6)),m.group(1),m.group(2)])
	  debug_info += ' ' + m.group(4) + ' ' + m.group(6) + '|' + m.group(1) + '|' + m.group(2) + "\n"
	elif m.group(5):
	  data_newest.append([int(m.group(5)),int(m.group(6)),m.group(1),m.group(2)])
	  debug_info += ' ' + m.group(5) + ' ' + m.group(6) + '|' + m.group(1) + '|' + m.group(2) + "\n"
## ---------------------------
## -- ETL Source 2: 
## -- News stories (front page)
    debug_info += "-----------------------\n" 
    data_news = []
    result = urlfetch.fetch(url='https://news.ycombinator.com/news',deadline=60)
    if result.status_code == 200:
      txt_data = result.content
      p = re.compile("<td class=.title.+?><a href=\"(.+?)\".*?>(.+?)<\/a>.+?((\d+) points?</span>|(\d+) \w+ ago</td>).+?<a href=.item\?id=(\d+).>")
      for m in p.finditer(txt_data):
	if m.group(4):
	  data_news.append([int(m.group(4)),int(m.group(6)),m.group(1),m.group(2)])
	  debug_info += ' ' + m.group(4) + ' ' + m.group(6) + '|' + m.group(1) + '|' + m.group(2) + "\n"
	elif m.group(5):
	  data_news.append([int(m.group(5)),int(m.group(6)),m.group(1),m.group(2)])
	  debug_info += ' ' + m.group(5) + ' ' + m.group(6) + '|' + m.group(1) + '|' + m.group(2) + "\n"
## ---------------------------
## -- Compare stories from both
## -- sources if they match
    debug_info += "-----------------------\n"
    data_both = []
    if len(data_news) >= 1 and len(data_newest) >= 1:
      for i in range(0,len(data_news)):
        for j in range(0,len(data_newest)):
	  if data_news[i][1] == data_newest[j][1]:
	    data_both.append([data_newest[j][0],data_newest[j][1],data_newest[j][2],data_newest[j][3]])
	    debug_info += ' ' + str(data_newest[j][0]) + ' ' + str(data_newest[j][1]) + '|' + data_newest[j][2] + '|' + data_newest[j][3] + "\n"
	    break
## ---------------------------
## -- Put results in DB
    debug_info += "-----------------------\n"
    if len(data_both) >= 1:
      stories_json = '['
      stories_n = len(data_both)
      stories_score = int(0);
      for i in range(0,len(data_both)):
	stories_score += data_both[i][0]
	if not re.match(r'http',data_both[i][2]):
	  data_both[i][2] = 'http://news.ycombinator.com/' + data_both[i][2]
	stories_json += '{"score":' + str(data_both[i][0]) + ',"id":' + str(data_both[i][1]) + ',"title":"' + str(data_both[i][3]) + '","url":"' + str(data_both[i][2]) + '"}'
	if i+1 < len(data_both):
	  stories_json += ','
      stories_json += ']'
      debug_info += stories_json
      etime_now = int(time.time()*1000)
      hnnoise = HNNOISE(etime=etime_now,stories_score=float(stories_score)/float(stories_n),stories_n=stories_n,stories_json=stories_json)
## ---------------------------
## -- Did we get it right?
    self.response.headers['Content-Type'] = 'text/plain'
    self.response.out.write(debug_info)

etl = webapp2.WSGIApplication([('/etl_process',ETLPage)],debug=True)

