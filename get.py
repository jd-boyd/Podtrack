from urlparse import urlparse
import httplib2

def getList():
    f = open('../get.sh', 'r')
    u=[]
    for l in f: u.append(l)
    
    u2=[]
    for l in u:
        w, url = l.split(' ')
        if url[-1]=='\n':
            url = url[:-1] #Trim the final newline
        u2.append(url)
    return u2

urlList = getList()

def getFile(url, filename):
    h = httplib2.Http(".cache")
    resp, data = h.request(u, "GET")
    
    f = open(fileName, 'w')
    f.write(data)
    f.close()

for u in urlList:
    print "Grabbing", u
    uInfo = urlparse(u)
    fileName = uInfo.path.split('/')[-1]
    print "Saving as", fileName

    getFile(u, fileName)
#    h = httplib2.Http(".cache")
#    resp, data = h.request(u, "GET")
#    
#    f = open(fileName, 'w')
#    f.write(data)
#    f.close()

