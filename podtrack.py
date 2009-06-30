#!/usr/bin/python

import sys, traceback
import feedparser
import datetime
from urlparse import urlparse
import httplib2
import gzip
import xml.parsers.expat

try:
    from sqlite3 import dbapi2 as sqlite
except:
    from pysqlite2 import dbapi2 as sqlite

#on ubuntu 8.10 use sqlite3 from CLI instead of just sqlite
#create table podCast ( pName char(50), pUrl  char(2048), pId INTEGER PRIMARY KEY AUTOINCREMENT);
#pUrl is the feed item url, while pHref is the enclosure URL
#create table podItems (pTitle char(1024), pUrl char(2048), pHref char(2048), pId integer, itemId integer primary key autoincrement, gotten integer);

#def createDb():
#    c = con.cursor()
#    c.execute("""create table podCast ( pName char(50), pUrl  char(2048), pId INTEGER PRIMARY KEY AUTOINCREMENT);""")
#    c.execute("""create table podItems (pTitle char(1024), pUrl char(2048), pHref char(2048), pId integer, itemId integer primary key autoincrement);""")
#    c.execute("""create table podConfig (pKey char(1024), pVal char(2048), cId integer primary key autoincrement);""")
#    con.commit()

#con=None

class podDb(object):
    def __init__(self):
        self.con=None

    def open(self):
        self.con = sqlite.connect('podtrack.db')

    def testDb(self):
        try:
            self.con.cursor().execute("select * from podCast")
            return True
        except sqlite.OperationalError, soe:
            return False

    def createDb(self):
        c = self.con.cursor()
        c.execute("""create table podCast ( pName char(50), pUrl  char(2048), pId INTEGER PRIMARY KEY AUTOINCREMENT);""")
        c.execute("""create table podItems (pTitle char(1024), pUrl char(2048), pHref char(2048), pId integer, itemId integer primary key autoincrement);""")
        c.execute("""create table podConfig (pKey char(1024), pVal char(2048), cId integer primary key autoincrement);""")
        self.con.commit()

    def isNewEntry(self, e):
        c = self.con.cursor()
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

    def addEntry(self, title, link, enc, pid):
        t={"t": title, "u": str(link), "h": str(enc), "i": pid}
        c = self.con.cursor()
        c.execute("insert into podItems(pTitle, pUrl, pHref, pId, gotten) values(:t,:u,:h,:i, 0);", t)
        self.con.commit()

    def addFeed(self, con, url):
        print "Adding", url, "to podtrackdb"
        f = feedparser.parse(url)
        t = f.feed.title
        print t
        self.con.cursor().execute('insert into podCast(pName, pUrl) values (:n, :u);', {'u': url, 'n': ''})
        self.con.commit()

    def isNewEntry(self, e):
        c = self.con.cursor()
        if 'title' in e:
            title = e['title'].encode("utf-8")
        else:
            title = 'New Item'
        if not 'enclosures' in e:
            return False

        t={"t": title, "u": str(e['link']), "h": str(e['enclosures'][0]['href'])}
        c.execute("select itemId from podItems where pTitle=:t and pUrl=:u and pHref=:h", t)
        ret=c.fetchall()

        if ret==[]:
            return True
        else:
            return False

class podcast(object):
    def __init__(self):
        #pName char(50), pUrl  char(2048), pId INTEGER PRIMARY KEY AUTOINCREMENT
        pass

filesToDl=[]

def getFeeds(fileName):
    # 3 handler functions
    feeds = []
    start_looking = False

    def start_element(name, attrs):
        if name=="outline":
           #print 'Start element:', name, attrs
            #print 'NAME:', attrs['text'], "\n  URL:", attrs['xmlUrl']
            feeds.append({"name": attrs['text'], "url": attrs['xmlUrl']})
            start_looking = True
    #def end_element(name):
    #    if name=="outline":
    #        start_looking = False
            #print 'End element:', name

    #def char_data(data):
    #    if start_looking:
    #        print 'Character data:', repr(data)

    p = xml.parsers.expat.ParserCreate()

    p.StartElementHandler = start_element
    #p.EndElementHandler = end_element
    #p.CharacterDataHandler = char_data

    f=open(fileName, 'r')
    d = f.read()
    f.close()

    p.Parse(d)
    return feeds

def processEntries(entries, pid):
    newItemAr=[]
    for e in entries:
        if pdb.isNewEntry(e):
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
                c = pdb.con.cursor()
                c.execute("insert into podItems(pTitle, pUrl, pHref, pId, gotten) values(:t,:u,:h,:i, 0);", t)
                pdb.con.commit()

            except KeyError, ke:
                print "ERROR----------------------------------"
                print "KEY ERROR:", ke.args[0]
                print e
    return newItemAr
        #else:
        #    print "Not new."

def fileNameFromUrl(url):
    uInfo = urlparse(url)
    return uInfo.path.split('/')[-1]

def getFile(url, filename):
    h = httplib2.Http(".cache")
    resp, data = h.request(u, "GET")
    
    f = open(filename, 'w')
    f.write(data)
    f.close()

from workPool import WorkerPoolSerial, WorkerPoolThreads

def grabFeed(l):
    print "Grabbing: ", l
    return feedparser.parse(l)

def makeGrabber(l):
    return lambda: grabFeed(l)

def makeListOfFilesToGet():
    newItems=[]
    wq = WorkerPoolThreads(10)

    rows=[r for r in pdb.con.cursor().execute("select * from podCast;")]

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

# def addFeed(con, url):
#     print "Adding", url, "to podtrackdb"
#     f = feedparser.parse(url)
#     t = f.feed.title
#     print t
#     con.cursor().execute('insert into podCast(pName, pUrl) values (:n, :u);', {'u': url, 'n': ''})
#     con.commit()

class opml(object):
    pass

def importOpml(fileName):
    feeds=getFeedsFromOpml(fileName)
    for f in feeds:
        print "Adding", f
        pdb.addFeed(con, f['url'])

def getFeedsFromOpml(fileName):
    # 3 handler functions
    feeds = []
    start_looking = False

    def start_element(name, attrs):
        if name=="outline":
           #print 'Start element:', name, attrs
            #print 'NAME:', attrs['text'], "\n  URL:", attrs['xmlUrl']
            feeds.append({"name": attrs['text'], "url": attrs['xmlUrl']})
            start_looking = True
    #def end_element(name):
    #    if name=="outline":
    #        start_looking = False
            #print 'End element:', name

    #def char_data(data):
    #    if start_looking:
    #        print 'Character data:', repr(data)

    p = xml.parsers.expat.ParserCreate()

    p.StartElementHandler = start_element
    #p.EndElementHandler = end_element
    #p.CharacterDataHandler = char_data

    f=open(fileName, 'r')
    d = f.read()
    f.close()

    p.Parse(d)
    return feeds

def dumpOpml(fileName):
    c = con.cursor()
    c.execute('select pName, pUrl from podCast order by pName;')
    f = open(fileName, 'w')
    
    f.write('<?xml version="1.0" encoding="ISO-8859-1"?>\n')
    f.write('<!-- OPML generated by podtrack on ' + str(datetime.datetime.now()) + ' -->\n')
    f.write('<opml version="1.1">\n')
    f.write('<body>\n')
    for r in c:
        line = '<outline text="'
        
        if r[0]=="":
            line += r[1]
        else:
            line += r[0]
        line += '" xmlUrl="'
        line += r[1] + '"/>\n'
        f.write(line)
    f.write('</body></opml>\n')
    f.close()

pdb = podDb()

if __name__ == "__main__":
    #con = sqlite.connect('podtrack.db')
    
    #try:
    #    con.cursor().execute("select * from podCast")
    #except sqlite.OperationalError, soe:
    #    createDb()


    pdb.open()

    args = getArgHash()

    argList = ['--add', '--list', '--opml', '--help', '--nothing']

    if '--add' in args:
        url = args['--add']
        addFeed(con, url)
        sys.exit(0)

    if '--list' in args:
        c = pdb.con.cursor()
        c.execute('select pName, pUrl from podCast order by pName;')
        print "List of podcasts:"
        for r in c:
            if r[0]=="":
                print r[1]
            else:
                print r[0]
        sys.exit(0)

    if '--exportOpml' in args:
        dumpOpml(args['--exportOpml'])
        sys.exit(0)       

    if '--importOpml' in args:
        importOpml(args['--importOpml'])
        sys.exit(0)

    if '--help' in args:
        print "No help yet:", args['--help']
        print argList
        sys.exit(0)

    if '--nothing' in args:
        print "Doing nothing:", args['--nothing']
        sys.exit(0)

    ar=makeListOfFilesToGet()

    #print repr(ar)
    #print repr(filesToDl)

    audioDir = pdb.con.cursor().execute('select pVal from podConfig;').fetchone()[0]

#for f in `ls` ; do mv $f `echo $f | sed 's/\?.*$//'` ; done
    f=open('get.sh', 'w')
    for u in ar: #filesToDl:
        f.write('wget ' + u + '\n')
    f.close()

    audioDir = pdb.con.cursor().execute('select pVal from podConfig;').fetchone()[0]
    for u in ar: #filesToDl:
        fileName = fileNameFromUrl(u)
        print "Downloading to ", audioDir + "/" + fileName, "from", u
        getFile(u, audioDir + "/" + fileName)

    

