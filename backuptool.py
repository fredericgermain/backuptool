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
			con.text_factory = str
		except sqlite3.Error, e:
		    print "Error %s:" % e.args[0]
		    sys.exit(1)
	    
		finally:
		    if con:
				self.con = con

		try:
			self.cur = self.con.cursor()    
			#self.cur.execute("DROP TABLE File")
			self.cur.execute("CREATE TABLE File(path TEXT, sha256 TEXT, mtime INT, PRIMARY KEY (path))")
    	
			data = self.cur.fetchone()
		except sqlite3.Error, e:
			#print "Error %s:" % e.args[0]
			#sys.exit(1)
			1

	def index_entry(self, path):
			if os.path.islink(path):
				pass
			elif os.path.isfile(path):
				mtime= os.path.getmtime(path)
				path_from_base = path[self.basepath_len+1:]
				self.cur.execute("SELECT * FROM File WHERE path = ?", (path_from_base,))
				rows = self.cur.fetchall()
				if len(rows):
					row = rows[0]
					if row[2] == mtime:
						print "file %s already in base, mtime same" % (path_from_base)
						hexhash = row[1]
					else:
						hexhash = sha256_for_file(path)
						if hexhash == row[1]:
							print "file %s already in base, mtime changed, content same" % (path_from_base)
						else:
							print "file %s already in base, mtime changed, content changed" % (path_from_base, row[2], mtime)
						self.cur.execute("UPDATE File SET sha256=?, mtime=? WHERE path = ?", (hexhash,mtime,path_from_base) )
						self.con.commit() 
					pass
				else:
					hexhash = sha256_for_file(path)
					print("file %s new, hash %s" % (path_from_base, hexhash))
					self.cur.execute("INSERT INTO File VALUES (?, ?, ?)", (path_from_base, hexhash, mtime) )
					self.con.commit() 
			elif os.path.isdir(path):
				self.index_directory(path)

	def index_directory(self, path):
		for sub_path in os.listdir(path):
			next_path = os.path.join(path, sub_path)
			self.index_entry(next_path)

	def merge_entry(self, path):
		if os.path.islink(path):
			pass
		elif os.path.isfile(path):
			hexhash = sha256_for_file(path)
			#print("file %s %s" % (path_from_base, hexhash))

			self.cur.execute("SELECT * FROM File WHERE sha256 = '%s'" % hexhash)
			rows = self.cur.fetchall()
			if len(rows):
				row = rows[0]
				print "%s match %s" % (path, row[1])
			else:
				print "no match %s %s" % (path, hexhash)

			self.con.commit() 
		elif os.path.isdir(path):
			self.merge_directory(path)

	def merge_directory(self, path):
		for sub_path in os.listdir(path):
			next_path = os.path.join(path, sub_path)
			self.merge_entry(next_path)

	def remove_removed_files(self):
		self.cur.execute("SELECT path FROM File")
		while 1 :
			row = self.cur.fetchone()
			if row is None:
				break
			fullpath = os.path.join(self.basepath, row[0])
			if not os.path.isfile(fullpath):
				print "file %s was deleted" (row[0])
				self.cur.execute("DELETE FROM File WHERE path = ?", row[0])

	def index(self):
		self.remove_removed_files()
		self.index_directory(self.basepath)

	def merge(self, path_to_merge):
		self.merge_entry(path_to_merge)

	def show_duplicates(self):
		self.cur.execute("SELECT sha256, path, mtime FROM File ORDER BY sha256, mtime")
		cursha256 = None
		files = None
		while 1 :
			row = self.cur.fetchone()
			if row is not None and row[0] == cursha256:
				files.append(row[1])
			else:
				if files is not None and len(files) > 1:
					print("%s"  %(cursha256))
					for file in files:
						print "  - %s" % (file)
				if row is None:
					break
				cursha256 = row[0]
				files = []



def usage():
	print "backuptool -p path"

def main(argv):                         
	repopath=None
	doindex=False
	domergepath=None
	doshowduplicates=False
	try:
		opts, args = getopt.getopt(argv, "hp:dim:", ["help", "path=", "index", "merge=", "show-duplicates"])
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
		elif opt in ("-d", "--show-duplicates"):
			doshowduplicates = True

	if repopath is None:
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
		print "t"
		if not os.path.isdir(domergepath):
			print "not a dir %s" % (domergepath)
			sys.exit(2)
		print "Merging %s" % (domergepath)
		repo.merge(domergepath)
	if doshowduplicates:
		print "Show duplicates %s" % (realrepopath)
		repo.show_duplicates()

if __name__ == "__main__":
    main(sys.argv[1:])