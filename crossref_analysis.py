#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os
import json
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
#import lxml.etree as et
import re

ppath = os.path.dirname(sys.argv[0]) # Расположение основного скрипта
#paper_collection = ppath+'/../texts/papers_crossref.txt'
paper_collection = ppath+'/../texts/small_collection.txt'
extract_abstract = re.compile(r'<jats:p>(.*?)</jats:p>', re.U)
jats_all = re.compile(r'(<\/?jats.*?>)', re.U)

def jsonProcessing(data) :
  paper_json = json.loads(data.strip())
  data = {}
  try :
    data['issue'] = paper_json['issue']
  except KeyError:
    pass
  try :
    data['volume'] = paper_json['volume']
  except KeyError:
    pass
  try :
    data['page'] = paper_json['page']
  except KeyError:
    pass
  try :
    data['published-print'] = paper_json['published-print']['date-parts'][0][0]
  except KeyError:
    pass
  try :
    data['published-online'] = paper_json['published-online']['date-parts'][0][0]
  except KeyError:
    pass
  try :
    data['subject'] = paper_json['subject']
  except KeyError:
    pass
  try :
    data['license'] = paper_json['license']
  except KeyError:
    pass
  try :
    data['funder'] = paper_json['funder']
  except KeyError:
    pass
  try :
    data['assertion'] = paper_json['assertion']
  except KeyError:
    pass
  try :
    data['author'] = paper_json['author']
  except KeyError:
    pass
  try :
    data['content-domain'] = paper_json['content-domain']
  except KeyError:
    pass
  try :
    data['publisher'] = paper_json['publisher']
  except KeyError:
    pass
  try :
    data['title'] = paper_json['title']
  except KeyError:
    pass
  try :
    data['references-count'] = paper_json['references-count']
  except KeyError:
    pass
  try :
    data['referenced'] = paper_json['is-referenced-by-count']
  except KeyError:
    pass
  try :
    data['issued'] = paper_json['issued']['date-parts'][0][0]
  except KeyError:
    pass
  try :
    data['type'] = paper_json['type']
  except KeyError:
    pass
  try :
    abstract = jats_all.sub('', paper_json['abstract'])
    abstract = abstract.replace('\n', '')
    abstract = abstract.replace('Abstract', '')
    abstract = abstract.replace('ABSTRACT', '')
    abstract = abstract.replace('Objective.', '')
    abstract = abstract.replace('Background   ', '')
    abstract = abstract.replace('Aims   ', '')
    abstract = abstract.replace('Methods.', '')
    abstract = abstract.replace('Method   ', '')
    abstract = abstract.replace('Results.', '')
    abstract = abstract.replace('Results   ', '')
    abstract = abstract.replace('Conclusions   ', '')
    abstract = abstract.replace('Conclusion.', '')
    abstract = abstract.replace('Summary   ', '')
    abstract = abstract.strip()
    data['abstract'] = abstract
  except (KeyError, IndexError) :
    pass
  return data

def readBigCollection(datafile=paper_collection) :
  with open(datafile) as largeobject :
    data = largeobject.readlines()
    data = list(map(jsonProcessing, data))
  df = pd.DataFrame(data)
  #df.to_parquet(datafile[:-3]+'parquet', engine='fastparquet', index=False)
  return df

def importPapersData(datafile=paper_collection) :
  df = pd.DataFrame()
  i = 0
  with open(datafile, 'r') as collection :
    for paper in collection :
      paper_json = json.loads(paper.strip())
      data = {}
      try :
        data['issue'] = paper_json['issue']
      except KeyError:
        pass
      try :
        data['volume'] = paper_json['volume']
      except KeyError:
        pass
      try :
        data['page'] = paper_json['page']
      except KeyError:
        pass
      try :
        data['published-print'] = paper_json['published-print']['date-parts'][0][0]
      except KeyError:
        pass
      try :
        data['published-online'] = paper_json['published-online']['date-parts'][0][0]
      except KeyError:
        pass
      try :
        data['subject'] = paper_json['subject']
      except KeyError:
        pass
      try :
        data['license'] = paper_json['license']
      except KeyError:
        pass
      try :
        data['funder'] = paper_json['funder']
      except KeyError:
        pass
      try :
        data['assertion'] = paper_json['assertion']
      except KeyError:
        pass
      try :
        data['author'] = paper_json['author']
      except KeyError:
        pass
      try :
        data['content-domain'] = paper_json['content-domain']
      except KeyError:
        pass
      try :
        data['publisher'] = paper_json['publisher']
      except KeyError:
        pass
      try :
        data['title'] = paper_json['title']
      except KeyError:
        pass
      try :
        data['references-count'] = paper_json['references-count']
      except KeyError:
        pass
      try :
        data['referenced'] = paper_json['is-referenced-by-count']
      except KeyError:
        pass
      try :
        data['issued'] = paper_json['issued']['date-parts'][0][0]
      except KeyError:
        pass
      try :
        data['type'] = paper_json['type']
      except KeyError:
        pass
      try :
        abstract = jats_all.sub('', paper_json['abstract'])
        abstract = abstract.replace('\n', '')
        abstract = abstract.replace('Abstract', '')
        abstract = abstract.replace('ABSTRACT', '')
        abstract = abstract.replace('Objective.', '')
        abstract = abstract.replace('Background   ', '')
        abstract = abstract.replace('Aims   ', '')
        abstract = abstract.replace('Methods.', '')
        abstract = abstract.replace('Method   ', '')
        abstract = abstract.replace('Results.', '')
        abstract = abstract.replace('Results   ', '')
        abstract = abstract.replace('Conclusions   ', '')
        abstract = abstract.replace('Conclusion.', '')
        abstract = abstract.replace('Summary   ', '')
        abstract = abstract.strip()
        data['abstract'] = abstract
      except (KeyError, IndexError) :
        pass

      df = df.append(data, ignore_index=True)
      i += 1
      if i % 10000 == 0 :
        print('Обработано {0} статей'.format(i))
        df.to_parquet(datafile[:-3]+'parquet', engine='fastparquet', index=False)
  return df

#print(df.head(10))
#print(df.info(verbose=True, null_counts=True))
#print(df['abstract'])
