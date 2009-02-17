#!/usr/bin/python
__version__ = "$Revision: 1.1.1.1 $"
#$Date: 2009-02-17 20:27:29 $
#$Author: jdboyd $
#$Name: not supported by cvs2svn $
#$Id: update.py,v 1.1.1.1 2009-02-17 20:27:29 jdboyd Exp $

import sys, traceback
import feedparser
import datetime
import psycopg2
import psycopg2.extras

def isNewEntry(feedId, entry):
    if not 'link' in entry:
        print "NO LINK:", repr(entry)
        return False
    d = dict(f=feedId, u=entry.link)
    c.execute("select id from px_items where feed_id=%(f)s and link=%(u)s",d);
    r = c.fetchone()
    #print r
    if r: return False
    else: return True

newItemSqlStr="insert into px_items (feed_id,link,title,content,dcdate,dccreator,dcsubject) values (%(f)s,%(l)s,%(t)s,%(c)s,%(d)s,%(cr)s,%(s)s);"

def processEntries(feedId, entries):
    newItemAr=[]
    for e in entries:
        if isNewEntry(feedId, e):
            link = e['link']
            if 'title' in e:
                title = e['title'].encode("utf-8")
            else:
                title = 'New Item'
            print "New item:", title, ":", link

            try:
                content=e.content[0].value.encode("utf-8")
            except:
                try:
                    content=e.summary.encode("utf-8")
                except:
                    content=""

            if 'updated_parsed' in e:
                try:
                    eup = e['updated_parsed']
                    if type(eup) is str:
                        eup = eup[:6]
                        date="%d-%02d-%02dT%02d:%02d:%02d+00:00" % eup 
                    else:
                        date = ""
                except:
                    print "FeedID:", feedId, "eup:",e['updated_parsed']
                    raise
            else:
                #except:
                date = ""

            try:
                print repr(e.author_detail)
                creator = e.author_detail.name.value.encode("utf-8")
            except:
                creator=""

            newItemDict = dict(f=feedId, l=link, t=title, c=content, d=date, cr=creator,s="")
            #print newItemDict
            newItemAr.append(newItemDict)
    print len(newItemAr), "new items."
    if len(newItemAr):
        for item in newItemAr:
            try:
                c.execute(newItemSqlStr, item)
            except psycopg2.IntegrityError, pie:
                print repr(pie)
                print repr(pie.args)
                print "Item to cause failure:", item
                traceback.print_exc()
                traceback.print_exc(file=fd)
        d.commit()

def processFeed(url, feedId):
    try:
        feed = feedparser.parse(url)
        try:
            print "ID:", feedId, repr(feed.feed.title), "items:", len(feed['entries'])
        except:
            print "URL doesn't support title?", url
        processEntries(feedId, feed['entries'])
    except:
        print "problem with:", url
        fd.write("problem with: " + url + "\n")
        traceback.print_exc()
        traceback.print_exc(file=fd)
        d.rollback()
        pass

if __name__=="__main__":
    d = psycopg2.connect(database='feedonfeed', user='feedonfeed')
    c= d.cursor(cursor_factory = psycopg2.extras.DictCursor) ;

    justArgs = sys.argv[1:]    
    arguments = {}    
    for a in justArgs:  
        k,v =a.split('=')        
        arguments[k]=v
    if '--feedId' in arguments:
        c.execute("select * from px_feeds where id=%(fid)s;", dict(fid=arguments['--feedId']))
    else:
        c.execute("select * from px_feeds order by title asc;")

    fd = open("problemFile", "a")
    for r in c.fetchall():
        processFeed(r['url'], r['id'])

    totalCountSqlStr="UPDATE px_feeds SET total_count = ( select count(px_items.feed_id) from px_items where px_items.feed_id=px_feeds.id group by px_items.feed_id);"
    c.execute(totalCountSqlStr)


    newCountSqlStr="UPDATE px_feeds SET new_count = ( select count(px_items.feed_id) from px_items where read is null and px_items.feed_id=px_feeds.id group by px_items.feed_id);"
    c.execute(newCountSqlStr)

    d.commit()
