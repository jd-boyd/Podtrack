import threading
import Queue
import sys

class WorkerPoolSerial(object):
    """Tasks are meant to be functions that take no arguments, say lambdas"""
    def __init__(self, threadCount=1):
        self.workQ=Queue.Queue()
        self.doneQ=Queue.Queue()
        self.threadCount=threadCount
        self.taskList = []


    def stop(self):
        pass

    def addTask(self, key, task):
        self.workQ.put((key, task))
        key, task=self.workQ.get()
        res = task()
        self.doneQ.put((key, res))

    def getComplete(self):
        if self.doneQ.empty():
            return None
        key, res=self.doneQ.get()
        return key, res

    def __iter__(self):
        res=self.getComplete()
        while res!=None:
            yield res
            res = self.getComplete()

class WorkerPoolThreads(object):
    """Tasks are meant to be functions that take no arguments, say lambdas.
       This class is meant to only be used by one thread, but the functions 
       you pass in must be thread safe.
    """
    def __init__(self, threadCount=1):
        self.workQ=Queue.Queue()
        self.doneQ=Queue.Queue()
        self.threadCount=threadCount
        self.threads=[]
        self.running=True
        self.taskList = []
        self.printWorkerStatus=False
        for i in range(threadCount):
            thread = threading.Thread(target=self.worker, args=(i,))
            thread.start()
            self.threads.append(thread)

    def stop(self):
        self.running=False
        if self.printWorkerStatus:
            print "Stop called."
        #self.workQ.join()
        for t in self.threads:
            t.join()

    def task_done(self):
        """This is an alternative function to use for older versions of
            Python than 2.5"""
        pass

    def worker(self, id):
        if sys.version_info[0]==2 and sys.version_info[1]<5:
            taskDone = self.task_done
        else:
            taskDone = self.workQ.task_done()
        while self.running:
            try:
                key, task=self.workQ.get(True, 1)
            except Queue.Empty:
                continue
            if self.printWorkerStatus:
                print "worker", id,"got task."
            taskDone()
            res = task()
            self.doneQ.put((key, res))
            if self.printWorkerStatus:
                print "worker",id,"done task"
        if self.printWorkerStatus:            
            print "worker",id,"ending"

    def addTask(self, key, task):
        self.taskList.append(key)
        self.workQ.put((key, task))

    def wrapFcn(self, fcn, args):
        if type(args) is list:
            return lambda: fcn(*args)
        if type(args) is dict:
            return lambda: fcn(**args)
            

    def addTaskArgs(self, key, fcn, args):
        self.taskList.append(key)
        task = self.wrapFcn(fcn, args)
        self.workQ.put((key, task))

    def getComplete(self):
        if not self.taskList:
            return None
        key, res=self.doneQ.get()
        self.taskList.remove(key)
        return key, res

    def __iter__(self):
        res=self.getComplete()
        while res!=None:
            yield res
            res = self.getComplete()

if __name__ == "__main__":
	import unittest

        class verifyWorkPool(unittest.TestCase):
            #def workFunc(self, x):
            #    return x*x

            def getWorkFunc(self, x):
                return lambda: x*x

            def qT(self, wp):
                input = range(10)
                output = [ x*x for x in input]
                
                for i in input:
                    wp.addTask(i, self.getWorkFunc(i))

                for key, ret in wp:
                    assert(ret in output)
                    output.remove(ret)
                assert(not len(output))
                wp.stop()

            def testQS(self):
                self.qT(WorkerPoolSerial(10))

            def testQT(self):
                self.qT(WorkerPoolThreads(10))


        unittest.main()
