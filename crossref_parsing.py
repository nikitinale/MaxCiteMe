#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script for parsing metainformation about articles from Crossref through API and saving it in JSON-like strings in text file
"""
import sys
import os
import json
from time import sleep
import random
from urllib import parse
from grab import Grab

base_url = 'https://api.crossref.org/works?'
# filter for parsing only articles published in journals, have abstracts and published until 2019
# for more information about filters see manual: https://github.com/CrossRef/rest-api-doc#filter-names
filter = 'filter=type:journal-article,has-abstract:t,until-pub-date:2019' 
ppath = os.path.dirname(sys.argv[0]) # path to the script location
paper_collection = ppath+'/../texts/papers_crossref.txt' # path to file for saving parsed data
ud_dois = ppath+'/../texts/ud_dois.txt' # # path to file with list of DOI to saved papers

# initialization and settings of grab object
g = Grab(log_file=ppath+'/../temp/out_crossref.html')
g.setup(cookiefile=ppath+'/../temp/cookies_pars.txt', reuse_referer='True', timeout=120)
g.setup(user_agent='CitePrediction/0.1_alpha; mailto:nikitinale@gmail.com')

# reading used DOIs from file and generating list with used DOIs for exclusion them from double retrieving
used_dois = []
with open(ud_dois, 'r') as dois :
  used_dois = dois.readlines()
used_dois = [x.strip() for x in used_dois]

#url = base_url+filter+'&rows=1000'+'&cursor=*' # parsing papers consequentially. Busting the same DOIs every time the script staring if cursor will not saved 
url = base_url+filter+'&sample=100' # random sample of papers from database. As much papers will parsed, as much doubles it got

# first batch of papers
g.go(url)
batch = g.doc.json

working = True
while working :
  #print(batch['message']['next-cursor'])
  for paper in batch['message']['items'] :
    if paper['DOI'] not in used_dois :
      print(paper['DOI'])
      with open(ud_dois, 'a') as dois:
        dois.write(paper['DOI'])
        dois.write('\n')
      used_dois.append(paper['DOI'])
      with open(paper_collection, 'a') as coll :
        json.dump(paper, coll, ensure_ascii=False)
        coll.write('\n')
    else :
      print('Used DOIs {}'.format(paper['DOI'])) 
  sleep(random.randrange(2,12))
  #next_cursor = parse.quote_plus(batch['message']['next-cursor'])
  #url = base_url+filter+'&rows=1000'+'&cursor='+next_cursor
  url = base_url+filter+'&sample=100'
  g.go(url)
  batch = g.doc.json
