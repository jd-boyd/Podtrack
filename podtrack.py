#!/usr/bin/python2.6

import sys, traceback
import feedparser
import datetime
from urlparse import urlparse
import httplib2
import gzip
import xml.parsers.expat

import logging
import optparse

try:
    from sqlite3 import dbapi2 as sqlite
except:
    from pysqlite2 import dbapi2 as sqlite

# on ubuntu 8.10 use sqlite3 from CLI instead of just sqlite

# create table podCast ( pName char(50), pUrl  char(2048), pId INTEGER PRIMARY KEY AUTOINCREMENT);

# pUrl is the feed item url, while pHref is the enclosure URL

# create table podItems (pTitle char(1024), pUrl char(2048), pHref
# char(2048), pId integer, itemId integer primary key autoincrement,
# gotten integer);

class PodDb(object):
    def __init__(self):
        self.con=None

    def open(self):
        self.con = sqlite.connect('podtrack.db')

    def testDb(self):
        try:
            self.con.cursor().execute(u"select * from podCast")
            return True
        except sqlite.OperationalError, soe:
            return False

    def createDb(self):
        c = self.con.cursor()
        c.execute(u"""create table podCast ( pName char(50), pUrl  char(2048), pId INTEGER PRIMARY KEY AUTOINCREMENT);""")
        c.execute(u"""create table podItems (pTitle char(1024), pUrl char(2048), pHref char(2048), pId integer, itemId integer primary key autoincrement);""")
        c.execute(u"""create table podConfig (pKey char(1024), pVal char(2048), cId integer primary key autoincrement);""")
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
        c.execute(u"select itemId from podItems where pTitle=:t and pUrl=:u and pHref=:h", t)
        ret=c.fetchall()

        if ret==[]:
            return True
        else:
            return False

    def addEntry(self, title, link, enc, pid):
        t={"t": title, "u": str(link), "h": str(enc), "i": pid}
        c = self.con.cursor()
        c.execute("insert into podItems(pTitle, pUrl, pHref, pId, gotten) values(:t,:u,:h,:i, 0);", t)
        self.con.commit()

    def addFeed(self, url):
        print "Adding", url, "to podtrackdb"
        f = feedparser.parse(url)
        t = f.feed.title

        self.con.cursor().execute('insert into podCast(pName, pUrl) values (:n, :u);', {'u': url, 'n': ''})
        self.con.commit()

    def isNewEntry(self, e):
        c = self.con.cursor()
        if 'title' in e:
            title = e['title']
        else:
            title = 'New Item'
        if not 'enclosures' in e:
            return False

        t=(unicode(title), 
           unicode(e['link']), 
           unicode(e['enclosures'][0]['href']))

        c.execute(u"select itemId from podItems where pTitle=? and pUrl=? and pHref=?", t)
        ret=c.fetchall()

        if ret==[]:
            return True
        else:
            return False
    def getConfigOption(self, key):
        t={"pk": key}
        return self.con.cursor().execute('select pVal from podConfig where pKey=:pk;', t).fetchone()[0]

class podcast(object):
    def __init__(self):
        #pName char(50), pUrl  char(2048), pId INTEGER PRIMARY KEY AUTOINCREMENT
        self.pid=0
#rows=[r for r in pdb.con.cursor().execute("select * from podCast;")]

#    for r in rows:
#        l = r[1]
#        print "Adding:",l, r[2]

        pass

    def processEntries(self):
        pass

filesToDl=[]

def processEntries(entries, pid):
    """Process all items in a collection of entries and return the list 
    of new URLs to fetch."""
    newItemAr=[]
    for e in entries:
        if pdb.isNewEntry(e):
            link = e['link']
            if 'title' in e:
                title = unicode(e['title'])
            else:
                title = u'New Item'
            logging.info(u"New item: %s : %s", title.encode("utf-8"), link)
            try:
                enc = e['enclosures'][0]['href']
                logging.info("Download: %s", enc)
                newItemAr.append(enc)
                
                t={"t": title, "u": unicode(link), "h": unicode(enc), "i": pid}
                c = pdb.con.cursor()
                c.execute(u"insert into podItems(pTitle, pUrl, pHref, pId, gotten) values(:t,:u,:h,:i, 0);", t)
                pdb.con.commit()

            except KeyError, ke:
                logging.error("ERROR----------------------------------")
                logging.error("KEY ERROR: %s", ke.args[0])
                logging.error("Working on entry: %s", e)
    return newItemAr

def fileNameFromUrl(url):
    uInfo = urlparse(url)
    return uInfo.path.split('/')[-1]

def getFile(url, filename):
#Review this function in light of: 
# http://diveintopython.org/http_web_services/index.html
    h = httplib2.Http(".cache")
    resp, data = h.request(url, "GET")
    
    f = open(filename, 'w')
    f.write(data)
    f.close()

from workPool import WorkerPoolSerial, WorkerPoolThreads

def grabFeed(l):
    logging.debug("Grabbing: %s", l)
    return feedparser.parse(l)

def makeGrabber(l):
    return lambda: grabFeed(l)

def makeListOfFilesToGet():
    newItems=[]
    wq = WorkerPoolThreads(10)

    rows=[r for r in pdb.con.cursor().execute("select * from podCast;")]

    for r in rows:
        l = r[1]
        logging.debug("Adding: %s %s",l, r[2])
        wq.addTask(r[2], makeGrabber(l))
        
    for r,feed in wq:
        logging.debug("PE: %s", r)
        newItems += processEntries(feed['entries'], r)

    wq.stop()
    return newItems

class opml(object):
    def __init__(self, pod_db):
        self.pdb = pod_db

    def importFromFile(self, fileName):
        feeds=getFeedsFromOpml(fileName)
        for f in feeds:
            print "Adding", f
            self.pdb.addFeed(f['url'])

    def getFeedsFromOpml(self, fileName):
        # 3 handler functions
        feeds = []
        start_looking = False

        p = xml.parsers.expat.ParserCreate()

        def start_element(name, attrs):
            if name=="outline":
                feeds.append({"name": attrs['text'], "url": attrs['xmlUrl']})
                start_looking = True
        p.StartElementHandler = start_element

        f=open(fileName, 'r')
        d = f.read()
        f.close()
        p.Parse(d)
        return feeds
    
    def dumpToFile(self,fileName):
        c = self.pdb.con.cursor()
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
    
def makeOptions():
    # Populate our options, -h/--help is already there for you.
    optp = optparse.OptionParser()
    optp.add_option('-v', '--verbose', dest='verbose', action='count',
                    help="Increase verbosity (specify multiple times for more)")
    optp.add_option('-a', '--add', dest='add', 
                    help="Add a URL to the podtrack database")
    optp.add_option('-l', '--list', dest='list', action="store_true",
                    help="List feeds in database")
    optp.add_option('-e', '--exportOpml', dest='export', default='',
                    help="Export database as an OPML file.")
    optp.add_option('-i', '--importOpml', dest='importO', default='',
                    help="Import an OPML file and add it to the database.")
    return optp

pdb = PodDb()

if __name__ == "__main__":
    pdb.open()

    if not pdb.testDb(): 
        print "Database not found, creating new file."
        pdb.createDb()

    optp = makeOptions()
    opts, args = optp.parse_args()
        
    log_level = logging.WARNING # default
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose >= 2:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level)

    if opts.add:
        url = opts.add
        pdb.addFeed(url)
        sys.exit(0)

    if opts.list:
        c = pdb.con.cursor()
        c.execute('select pName, pUrl, pId from podCast order by pId;')
        print "List of podcasts:"
        for r in c:
            print "%3d)" % ( r[2],) ,
            if r[0]=="":
                print r[1]
            else:
                print r[0]
        sys.exit(0)

    if opts.export:
        opml(pdb).dumpToFile(opts.export)
        sys.exit(0)       

    if opts.importO:
        opml(pdb).importFromFile(opts.importO)
        sys.exit(0)

    ar=makeListOfFilesToGet()

    audioDir = pdb.con.cursor().execute('select pVal from podConfig where pKey="audioDir";').fetchone()[0]
    for u in ar: 
        fileName = fileNameFromUrl(u)
        d_path = audioDir + "/" + fileName
        logging.info("Downloading to %s from %s", d_path, u)
        getFile(u, d_path)

    

