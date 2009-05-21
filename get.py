from urlparse import urlparse
import httplib2, sys, gzip

try:
    from sqlite3 import dbapi2 as sqlite
except:
    from pysqlite2 import dbapi2 as sqlite


def getList():
    f = open('get.sh', 'r')
    u=[]
    for l in f: u.append(l)
    
    u2=[]
    for l in u:
        w, url = l.split(' ')
        if url[-1]=='\n':
            url = url[:-1] #Trim the final newline
        u2.append(url)
    return u2

def getFile(url, filename):
    h = httplib2.Http(".cache")
    resp, data = h.request(u, "GET")

    f = open(filename, 'w')
    f.write(data)
    f.close()

def fileNameFromUrl(url):
    uInfo = urlparse(url)
    return uInfo.path.split('/')[-1]

if __name__ == "__main__":
    urlList = getList()
    
    con = sqlite.connect('podtrack.db')
    audioDir = con.cursor().execute('select pVal from podConfig;').fetchone()[0]
    
    for u in urlList:
        print "Grabbing", u
        fileName = fileNameFromUrl(u)
        print "Saving as", audioDir + "/" + fileName

        getFile(u, audioDir + "/" + fileName)
