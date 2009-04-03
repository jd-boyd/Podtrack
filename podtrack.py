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
    c.execute("""create table podCast ( pName char(50), pUrl  char(2048), pId INTEGER PRIMARY KEY AUTOINCREMENT);""")
    c.execute("""create table podItems (pTitle char(1024), pUrl char(2048), pHref char(2048), pId integer, itemId integer primary key autoincrement);""")
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

#filesToDl=[]

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
                #filesToDl.append(enc)
                newItemAr.append(enc)
                t={"t": title, "u": str(link), "h": str(enc), "i": pid}
                #print repr(t)
                c = con.cursor()
                c.execute("insert into podItems(pTitle, pUrl, pHref, pId) values(:t,:u,:h,:i);", t)
                con.commit()
            except KeyError, ke:
                print "ERROR----------------------------------"
                print "KEY ERROR:", ke.args[0]
                print e
    return newItemAr
        #else:
        #    print "Not new."

from workPool import WorkerPoolSerial, WorkerPoolThreads

def grabFeed(l):
    print "Grabbing: ", l
    return feedparser.parse(l)

def makeGrabber(l):
    return lambda: grabFeed(l)

def makeListOfFilesToGet():
    newItems=[]
    wq = WorkerPoolThreads(10)

    rows=[r for r in con.cursor().execute("select * from podCast;")]

    for r in rows:
        l = r[1]
        print "Adding:",l, r[2]
        wq.addTask(r[2], makeGrabber(l))
        
    for r,feed in wq:
        print "PE:", r
        newItems += processEntries(feed['entries'], r)

    wq.stop()
    return newItems
    
def getArgHash():
    justArgs = sys.argv[1:]    
    arguments = {}    
    for a in justArgs:  
        try:
            arg_split = a.split('=')        
            k = arg_split[0]
            v = "=".join(arg_split[1:])
        except ValueError:
            k = a
            v = True
        arguments[k]=v
    #print "Args:", arguments
    return arguments

def addFeed(con, url):
    print "Adding", url, "to podtrackdb"
    f = feedparser.parse(url)
    t = f.feed.title
    print t
    con.cursor().execute('insert into podCast(pName, pUrl) values (:n, :u);', {'u': url, 'n': ''})
    con.commit()

if __name__ == "__main__":
    #wq = WorkerPoolSerial(10)
    con = sqlite.connect('podtrack.db')

    args = getArgHash()

    if '--add' in args:
        url = args['--add']
        addFeed(con, url)
        #print "Adding", url, "to podtrackdb"
        #f = feedparser.parse(url)
        #t = f.feed.title
        #print t
        #con.cursor().execute('insert into podCast(pName, pUrl) values (:n, :u);', {'u': url, 'n': ''})
        #con.commit()

    if '--nothing' in args:
        print "Doing nothing:", args['--nothing']
        sys.exit(0)

    ar=makeListOfFilesToGet()

    #print repr(ar)
    #print repr(filesToDl)

#for f in `ls` ; do mv $f `echo $f | sed 's/\?.*$//'` ; done
    f=open('get.sh', 'w')
    for l in ar: #filesToDl:
        f.write('wget ' + l + '\n')
    f.close()

