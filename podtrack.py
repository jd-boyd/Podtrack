#!/usr/bin/python

import sys, traceback
import feedparser
import datetime

try:
    from sqlite3 import dbapi2 as sqlite
except:
    from pysqlite2 import dbapi2 as sqlite

#on ubuntu 8.10 use sqlite3 from CLI instead of just sqlite
#create table podCast ( pName char(50), pUrl  char(2048), pId INTEGER PRIMARY KEY AUTOINCREMENT);
#pUrl is the feed item url, while pHref is the enclosure URL
#create table podItems (pTitle char(1024), pUrl char(2048), pHref char(2048), pId integer, itemId integer primary key autoincrement);

def createDb():
    c = con.cursor()
    c.execute("create table podCast ( pName char(50), pUrl  char(2048), pId INTEGER PRIMARY KEY AUTOINCREMENT);")
    c.execute("create table podItems (pTitle char(1024), pUrl char(2048), pHref char(2048), pId integer, itemId integer primary key autoincrement);")
    con.commit()

con=None

def isNewEntry(e):
    c = con.cursor()
    if 'title' in e:
        title = e['title'].encode("utf-8")
    else:
        title = 'New Item'
    if not 'enclosures' in e:
        return False

    t={"t": title, "u": str(e['link']), "h": str(e['enclosures'][0]['href'])}
    c.execute("select itemId from podItems where pTitle=:t and pUrl=:u and pHref=:h", t)
    ret=c.fetchall()
    #print repr(ret)
    if ret==[]:
        return True
    else:
        return False

filesToDl=[]

def processEntries(entries, pid):
    newItemAr=[]
    for e in entries:
        if isNewEntry(e):
            link = e['link']
            if 'title' in e:
                title = e['title'].encode("utf-8")
            else:
                title = 'New Item'
            print "New item:", title, ":", link
            try:
                enc = e['enclosures'][0]['href']
                print "Download:", enc
                filesToDl.append(enc)
                t={"t": title, "u": str(link), "h": str(enc), "i": pid}
                #print repr(t)
                c = con.cursor()
                c.execute("insert into podItems(pTitle, pUrl, pHref, pId) values(:t,:u,:h,:i);", t)
                con.commit()
            except KeyError, ke:
                print "ERROR----------------------------------"
                print "KEY ERROR:", ke.args[0]
                print e
        #else:
        #    print "Not new."

from workPool import WorkerPoolSerial, WorkerPoolThreads

def grabFeed(l):
    print "Grabbing: ", l
    return feedparser.parse(l)

def makeGrabber(l):
    return lambda: grabFeed(l)

def makeListOfFilesToGet():
    wq = WorkerPoolThreads(10)

    rows=[r for r in con.cursor().execute("select * from podCast;")]

    for r in rows:
        l = r[1]
        print "Adding:",l, r[2]
        wq.addTask(r[2], makeGrabber(l))
        
    for r,feed in wq:
        print "PE:", r
        processEntries(feed['entries'], r)

    wq.stop()
    
if __name__ == "__main__":
    #wq = WorkerPoolSerial(10)
    con = sqlite.connect('podtrack.db')
    makeListOfFilesToGet()

#for f in `ls` ; do mv $f `echo $f | sed 's/\?.*$//'` ; done
    f=open('get.sh', 'w')
    for l in filesToDl:
        f.write('wget ' + l + '\n')
    f.close()

