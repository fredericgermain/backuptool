#!/usr/bin/python

import getopt
import os
import sys

import sqlite3
import hashlib

def sha256_for_file(path, block_size=2**20):
    hash = hashlib.sha256()
    f = open(path, 'rb')
    while True:
        data = f.read(block_size)
        if not data:
            break
        hash.update(data)
    f.close()

    return hash.hexdigest()


class Repository:
	def __init__(self, basepath):
		self.basepath = basepath
		self.basepath_len = len(basepath)
		self.open_db()

	def open_db(self):
		dbpath = "%s.backuptool.db" % (self.basepath)

		try:
			con = sqlite3.connect(dbpath)

		except sqlite3.Error, e:
		    print "Error %s:" % e.args[0]
		    sys.exit(1)
	    
		finally:
		    if con:
				self.con = con

		try:
			self.cur = self.con.cursor()    
			self.cur.execute("CREATE TABLE File(sha256 TEXT, path TEXT)")
    	
			data = self.cur.fetchone()
		except sqlite3.Error, e:
			#print "Error %s:" % e.args[0]
			#sys.exit(1)
			1

	def index_path(self, path):
		for sub_path in os.listdir(path):
			next_path = os.path.join(path, sub_path)
			if os.path.islink(next_path):
				pass
			elif os.path.isfile(next_path):
				hexhash = sha256_for_file(next_path)
				path_from_base = next_path[self.basepath_len+1:]
				#print("file %s %s" % (path_from_base, hexhash))

				self.cur.execute("SELECT * FROM File WHERE sha256 = '%s'" % hexhash)
				rows = self.cur.fetchall()
				if len(rows):
					row = rows[0]
					if row[1] == path_from_base:
						#print "already in base %s" % (len(rows))
						pass
					else:
						print "%s already match %s" % (path_from_base, row[1])
						sys.exit(1)

				self.cur.execute("INSERT INTO File VALUES (?, ?)", (hexhash,path_from_base) )

				self.con.commit() 
			elif os.path.isdir(next_path):
				self.index_path(next_path)

	def merge_path(self, path):
		for sub_path in os.listdir(path):
			next_path = os.path.join(path, sub_path)
			if os.path.islink(next_path):
				pass
			elif os.path.isfile(next_path):
				hexhash = sha256_for_file(next_path)
				#print("file %s %s" % (path_from_base, hexhash))

				self.cur.execute("SELECT * FROM File WHERE sha256 = '%s'" % hexhash)
				rows = self.cur.fetchall()
				if len(rows):
					row = rows[0]
					print "%s match %s" % (next_path, row[1])

				self.con.commit() 
			elif os.path.isdir(next_path):
				self.merge_path(next_path)

	def index(self):
		self.index_path(self.basepath)

	def merge(self, path_to_merge):
		self.merge_path(self.basepath)


def usage():
	print "backuptool -p path"

def main(argv):                         
	repopath=None
	doindex=False
	domergepath=None
	try:
		opts, args = getopt.getopt(argv, "hp:dim:", ["help", "path=", "index", "merge="])
	except getopt.GetoptError:
		usage()
		sys.exit(2)
	for opt, arg in opts:
		if opt in ("-h", "--help"):
			usage()
			sys.exit()
		elif opt == '-d':
			global _debug
			_debug = 1
		elif opt in ("-p", "--path"):
			repopath = arg
		elif opt in ("-i", "--index"):
			doindex = True
		elif opt in ("-m", "--merge"):
			domergepath = arg

	if repopath == "":
		usage()
		sys.exit(2)

	realrepopath = os.path.realpath(repopath)

	if not os.path.isdir(realrepopath):
		print("could not find %s or not a directory" % (repopath))
		sys.exit(2)

	repo = Repository(realrepopath)
	if doindex:
		print "Reindexing %s" % (realrepopath)
		repo.index()
	if domergepath:
		if not os.path.isdir(domergepath):
			print "not a dir %s" % (domergepath)
			sys.exit(2)
		print "Merging %s" % (domergepath)
		repo.merge(domergepath)

if __name__ == "__main__":
    main(sys.argv[1:])