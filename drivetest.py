# -*- mode: python; coding: utf-8-*- vim:shiftwidth=4:expandtab
##############################################################################
"""
Copyright (C) 2011 Jörg Engelhart
license: GNU General Public License, version 2

Test hard disks / NAS for performance and errors with near-use random
access and/or get more confidence by advancing through infant
failures/mortality ( http://en.wikipedia.org/wiki/Bathtub_curve ).

See https://github.com/engelj/drivetest for further information.

Changelog:

  future: create a threaded version which fills the disk up and
    read/write files of varying size in parallel; check S.M.A.R.T log
    for temperature and failures;

  110422: first version, creation of files and checksums, check
    checksums
"""

import sys
import logging as log
import os
import platform
import ctypes
import threading
import time
import re
import tempfile
import random
import datetime
import optparse
import hashlib


def humanValue(v):

    for i, s in [[4, 'T'], [3, 'G'], [2, 'M'], [1, 'K']]:
        if v > 1024**i:
            return '%.2f %s' % (1.0 * v / 1024**i, s)
    return v


class File(object):

    reNameAttr = re.compile('--(\w+)-([^_]+)*')
    reNamePrefix = re.compile('(^[^-]+)')

    def __init__(self, test, name=None, size=None):

        self.test = test
        self.name = name
        self.size = size
        self.fh = None

    def humanStats(self):

        r = 'file %s, size: %s, speed: %s/s' % \
            (self.name, humanValue(self.size), humanValue(self.speed()))
        return r

    def getNamePrefix(self):

        r = self.reNamePrefix.match(self.name).group(1)
        log.debug('getNamePrefix of %s: %s' % (self.name, r))
        return r

    def setNameAttr(self, attr, val):
        "rename file such that it holds attr with value in its name"

        assert not self.name is None
        attrs = dict(self.reNameAttr.findall(self.name))
        attrs[attr] = val
        name = '%s--%s' % (self.getNamePrefix(),
                           '__'.join(['-'.join([k, v])
                                      for k, v in attrs.items()]))
        log.debug('new name: %s' % name)
        if name != self.name:
            os.rename(self.name, name)

    def getNameAttr(self, attr):
        "return attr from file name; if it is not contained returns None"

        assert not self.name is None
        attrs = dict(self.reNameAttr.findall(self.name))
        return attrs.get(attr)

    def randomSize(self):

        s = min(self.test.maxSize, self.test.freeSpace() - self.test.minFree)
        if s > 0:
            self.size = random.randrange(1024, self.test.maxSize)
        else:
            self.size = None

    def randomName(self):
        """
        set a random name (full path including root)

        provide a number of directories to avoid shortage of file allocation
        table entries
        """

        name = self.test.root
        for d in range(random.randrange(0, self.test.maxDepth + 1) + 1):
            name = tempfile.mkdtemp('', '', name)
        self.fh, self.name = tempfile.mkstemp('', '', name)
        log.debug('name: %s', self.name)

    def create(self):
        """
        write chunks of fixed size and measure the time needed;
        fill up to real size which was requested
        """

        rs = len(self.test.randoms[0])
        n = self.size / rs
        self.t1 = datetime.datetime.now()
        if self.fh is None:
            self.fh = open(self.name, 'wb')
        for i in range(n):
            os.write(self.fh, self.test.randoms[random.randrange(
                        len(self.test.randoms))])
        os.write(self.fh, self.test.randoms[0][(self.size - n * rs):])
        os.close(self.fh)
        self.t2 = datetime.datetime.now()
        self.size = os.stat(self.name).st_size
        self.fh = None

    def speed(self):

        assert self.t1
        assert self.t2
        s = (self.t2 - self.t1).microseconds
        return 1. * self.size / s * 1e6

    def checksum(self):

        md5 = hashlib.md5()
        self.t1 = datetime.datetime.now()
        f = open(self.name)
        while True:
            c = f.read(256*1024*1024)
            if c == '':
                break
            md5.update(c)
        self.cs = md5.hexdigest()
        f.close()
        self.t2 = datetime.datetime.now()
        self.size = os.stat(self.name).st_size


class Worker(threading.Thread):

    def __init__(self, test):

        threading.Thread.__init__(self)

        assert isinstance(test, DriveTest)
        self.test = test

    def run(self):
        pass


class Checker(Worker):
    """
    worker which can create/validate checksums
    """

    def __init__(self, test, file):
        Worker.__init__(self, test=test)
        assert isinstance(file, File)
        self.file = file
        self.cs = None

    def run(self):
        self.cs = self.file.checksum()


class Creator(Worker):
    """
    worker which can create a file
    """

    def __init__(self, test):
        Worker.__init__(self, test=test)
        self.file = None

    def run(self):
        self.file = File(test=self.test)
        self.file.randomName()
        self.test.files[self.file.name] = self.file
        self.file.randomSize()
        if not self.size is None:
            self.file.create()
            log.debug('%d: wrote %sB at %sB/s',
                      n, humanValue(self.file.speed()),
                      humanValue(self.file.size))


class DriveTest(object):

    """
    test a hard disk or a file share

    """

    def __init__(self, root, maxSize, maxDepth, minFree, threads):

        self.root = os.path.join(os.path.abspath(root), 'drivetest')
        self.maxSize = maxSize
        self.maxDepth = maxDepth
        self.minFree = minFree
        self.threads = threads

        if not os.path.isdir(self.root):
            os.makedirs(self.root)

        self.files = {}
        self.randoms = []
        self.workers = []

    def freeSpace(self):
        """
        free space on filesystem

        see http://stackoverflow.com/questions/51658/cross-platform-space-
        remaining-on-volume-using-python
        """

        if platform.system() == 'Windows':
            free = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.\
                GetDiskFreeSpaceExW(ctypes.c_wchar_p(self.root), None, None,
                                    ctypes.pointer(free))
            return free.value
        else:
            s = os.statvfs(self.root)
            # take bavail instead of bfree, as we might run non-root
            return s.f_bavail * s.f_bsize

    def avgSpeed(self):
        return sum([f.speed() for f in self.files.values()])/len(self.files)

    def createRandomBlocks(self):
        "creates a list of long random strings upfront to speed up file write"

        n = 100
        s = 512*1024
        log.info('create %d randoms blocks of size %sbytes', n, humanValue(s))
        for i in range(n):
            self.randoms += [random._urandom(s)]

    def findFiles(self):

        self.files = {}
        for dirpath, dirnames, filenames in os.walk(self.root):
            for f in filenames:
                ff = File(test=self, name=os.path.join(dirpath, f))
                self.files[ff.getNamePrefix()] = ff

    def getFileStats(self, files=None):

        log.info('get file statistics')
        if files is None:
            files = self.files.values()
        for f in files:
            try:
                f.stats = os.stat(f.name)
            except:
                f.stats = None

    def createfiles(self):
        """
        create files

        may be interrupted via Ctrl-C
        """

        fs = self.freeSpace()
        if fs < self.minFree:
            log.info('didnt start due to hit of free space limit')
            return

        self.createRandomBlocks()
        try:
            n = 0
            while True:
                f = File(self)
                f.randomName()
                self.files[f.name] = f
                f.randomSize()
                if f.size is None:
                    log.info('stopped due to hit of free space limit')
                    f.t2 = f.t1
                    break
                f.create()
                log.info('file %d: %s' % (n, f.humanStats()))
                n += 1
        except KeyboardInterrupt:
            pass

        print
        if len(self.files):
            log.info('average speed: %sB/s' % humanValue(self.avgSpeed()))
        return 0

    def createchecksums(self):
        "create checksums for all files"

        if len(self.files) == 0:
            self.findFiles()
        log.info('found %d files overall', len(self.files))

        # remove files which already have a checksum name
        files = list(set(self.files.values()) - \
                         set([f for f in self.files.values() \
                                  if f.getNameAttr('cs')]))
        lf = len(files)
        log.info('found %d files which still need to be checksummed', lf)
        random.shuffle(files)

        # determine modified times of every file two times
        # only the files which had no write access are taken
        t = datetime.datetime.now()
        self.getFileStats(files=files)
        for f in files:
            log.info('check for checksum: %s', f.name)
            f.mtime1 = f.stats.st_mtime
        dt = - ((datetime.datetime.now() - t) -
                datetime.timedelta(seconds=10)).seconds
        if dt > 0:
            time.sleep(dt)
        self.getFileStats()
        for f in files:
            if f.stats:
                f.mtime2 = f.stats.st_mtime

        # calculate checksums
        n = 0
        for f in files:
            if hasattr(f, 'mtime1') and hasattr(f, 'mtime2') and \
                    f.mtime1 == f.mtime2:
                n += 1
                # try:
                f.checksum()
                f.setNameAttr('cs', f.cs)
                log.info('file %d of %d: %s' % (n, lf, f.humanStats()))
                # except:
                #     pass
        log.info('calculated %d checksums', n)
        return 0

    def checkchecksums(self):

        self.findFiles()
        log.info('found %d files', len(self.files))

        # remove files which don't have a checksum name
        files = self.files.values()
        lf = len(files)
        random.shuffle(files)
        for f in files:
            if f.getNameAttr('cs') is None:
                del(self.files[f.getNamePrefix()])

        # check checksums
        n = 0
        for f in self.files.values():
            n += 1
            cs = f.getNameAttr('cs')
            f.checksum()
            if cs == f.cs:
                log.info('checksum ok: file %d of %d, %s' % \
                             (n, lf, f.humanStats()))
            else:
                log.error('wrong checksum: file %d of %d, %s' % \
                             (n, lf, f.humanStats()))
        log.info('checked %d checksums', n)

    def deleteEmptyDirs(self):
        "delete empty dirs"

        n = 0
        for dirpath, dirnames, filenames in os.walk(self.root, topdown=False):
            for d in dirnames:
                d = os.path.join(dirpath, d)
                if not os.listdir(d):
                    log.debug('remove directory %s', d)
                    os.rmdir(d)
                    n += 1
        log.info('deleted %d directories', n)

    def stress(self):
        """
        continuously create files and check checksums, watch for sufficient
        disk space by deleting files from a midrange of age (anticipating that
        old files are valuable as being most sensitive to fail)
        """

        self.createRandomBlocks()
        if len(self.files) == 0:
            self.findFiles()
        self.workers = []
        try:
            while True:
                fs = self.freeSpace()

                if fs < self.minFree - 2 * self.maxSize:
                    # if free space is less than two times the maximum
                    # file size delete sth
                    log.info('crossed space limit, begin to delete')
                    self.deleteMiddleAgedFile()
                else:
                    # create new files
                    if len(self.workers) < self.threads:
                        c = Creator(test=self)
                        c.start()
                        self.workers += [c]
                    # look for files which need to be checksummed

        except KeyboardInterrupt:
            for w in self.workers:
                w.stop()
        return 0


# main, only if standalone and not within ipython
if __name__ == '__main__' and not '__IP' in dir():
    parser = optparse.OptionParser()
    parser.add_option('--root', default='.')
    parser.add_option('--debug', action='store_true', help='show debug '
                      'information')
    parser.add_option('--max_size', type=float, default=2048, help='maximum '
                      'file size, you may use scientific float notation')
    parser.add_option('--max_depth', type=int, default=4, help='maximum '
                      'directory depth to avoid file allocation table '
                      'overflow')
    parser.add_option('--min_free', type=float, default=1024**3,
                      help='number of bytes to remain free on filesystem, '
                      'you may use scientific notation')
    parser.add_option('--threads', type=int, default=1, help='number of '
                      'threads')
    (opts, args) = parser.parse_args()

    if opts.debug:
        log.basicConfig(level=log.DEBUG, format='%(asctime)-15s %(levelname)s'
                        '(%(filename)s:%(lineno)d): %(message)s ')
    else:
        log.basicConfig(level=log.INFO, format='%(asctime)-15s %(levelname)s'
                        ': %(message)s ')

    cmds = ['stress', 'createfiles', 'createchecksums', 'checkchecksums',
            'deleteemptydirs']
    if len(args) != 1 or not args[0] in cmds:
        log.error('you need to specify a command, one of %s', ', '.join(cmds))
        log.info('use -h to get help information')
        sys.exit(1)
    cmd = args[0]

    opts.max_size = int(opts.max_size)
    if opts.max_size < 1024:
        log.error('max_size must be greater than 1024')
        sys.exit(1)

    d = DriveTest(root=opts.root, maxSize=opts.max_size,
                  minFree=opts.min_free, maxDepth=opts.max_depth,
                  threads=opts.threads)
    sys.exit(getattr(d, cmd)())
