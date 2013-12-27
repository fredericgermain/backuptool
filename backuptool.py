#!/usr/bin/python

import getopt
import os
import sys

import sqlite3
import hashlib
import shutil

backupignore = [ "desktop.ini", "thumbs.db", ".picasa.ini" ]

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

def mycopyfile(src, dst):
	dstpath = os.path.dirname(dst)
	if not os.path.isdir(dstpath):
		os.makedirs(dstpath)
	#copy content + metadata
	shutil.copy2(src, dst)

class MatchContext:
 	def __init__(self, basepath, domatchdeleteifmatch, mappath):
 		self.basepath = basepath
 		self.domatchdeleteifmatch = domatchdeleteifmatch
 		self.mappath = mappath


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

	def path_to_ignore(self, path):
		return os.path.basename(path).lower() in backupignore

	def index_entry(self, path):
			if os.path.islink(path):
				pass
			elif os.path.isfile(path):
				if self.path_to_ignore(path):
					return
				mtime= os.path.getmtime(path)
				path_from_base = path[self.basepath_len+1:]
				self.cur.execute("SELECT * FROM File WHERE path = ?", (path_from_base,))
				rows = self.cur.fetchall()
				if len(rows):
					row = rows[0]
					if row[2] == mtime:
						#print "file %s already in base, mtime same" % (path_from_base)
						hexhash = row[1]
					else:
						hexhash = sha256_for_file(path)
						if hexhash == row[1]:
							print "file %s already in base, mtime changed, content same" % (path_from_base)
						else:
							print "file %s already in base, mtime changed, content changed" % (path_from_base)
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

	def remove_removed_files(self):
		self.cur.execute("SELECT path FROM File")
		deleted = []
		while 1 :
			row = self.cur.fetchone()
			if row is None:
				break
			fullpath = os.path.join(self.basepath, row[0])
			if not os.path.isfile(fullpath) or self.path_to_ignore(row[0]):
				print "file %s was deleted" % (row[0])
				deleted.append(row[0])
			else:
				pass
		for path in deleted: 
			self.cur.execute("DELETE FROM File WHERE path = ?", (path,))
		self.con.commit() 

	def index(self):
		self.remove_removed_files()
		self.index_directory(self.basepath)

	def match_entry(self, matchcontext, path):
		if os.path.islink(path):
			pass
		elif os.path.isfile(path):
			if self.path_to_ignore(path):
				return
			hexhash = sha256_for_file(path)
			#print("file %s %s" % (path_from_base, hexhash))

			self.cur.execute("SELECT path FROM File WHERE sha256 = '%s'" % hexhash)
			rows = self.cur.fetchall()
			path_from_base = path[len(matchcontext.basepath)+1:]
			if len(rows):
				row = rows[0]
				if matchcontext.domatchdeleteifmatch:
					os.unlink(path)
				else:
					print "good match %s\n\t-> %s" % (path_from_base, row[0])
			else:
				if matchcontext.mappath is None:
					print "no match %s" % (path_from_base)
				else:
					print "no match %s -> %s" % (path_from_base, os.path.join(matchcontext.mappath, path_from_base))
					mycopyfile(path, os.path.join(self.basepath, matchcontext.mappath, path_from_base))

		elif os.path.isdir(path):
			self.match_directory(matchcontext, path)

	def match_directory(self, matchcontext, path):
		for sub_path in os.listdir(path):
			next_path = os.path.join(path, sub_path)
			self.match_entry(matchcontext, next_path)

	def match(self, matchcontext):
		self.match_entry(matchcontext, matchcontext.basepath)

	def show_duplicates(self, basepath, domatchdelete):
		self.cur.execute("SELECT sha256, path, mtime FROM File ORDER BY sha256, mtime")
		cursha256 = None
		files = None
		while 1 :
			row = self.cur.fetchone()
			if row is not None and row[0] == cursha256:
				if basepath is not None:
					if row[1].startswith(basepath):
						files.append(row[1])
				else:
					files.append(row[1])
			else:
				if files is not None and len(files) > 1:
					file_with_max_len = None
					print("%s"  %(cursha256))
					for file in files:
						print "  - %s" % (file)
						if file_with_max_len is None or len(file) > len(file_with_max_len):
							file_with_max_len = file
					if domatchdelete:
						print "keeping %s" % (file_with_max_len)
						for file in files:
							if file_with_max_len != file:
								print "unlink %s" % (file)
								os.unlink(os.path.join(self.basepath, file))


				if row is None:
					break

				cursha256 = row[0]
				files = []
				if basepath is not None:
					if row[1].startswith(basepath):
						files.append(row[1])
				else:
					files.append(row[1])



def usage():
	print "backuptool -p path [--show-duplicates] [--match path]"
	print "  --show-duplicates: show duplicates in index"
	print "  --match: match all files in path with file in index"

def main(argv):                         
	repopath=None
	doindex=False
	doshowduplicates=False
	domatchpath=None
	matchmappath=None
	domatchdelete=False
	try:
		opts, args = getopt.getopt(argv, "hp:im:d", ["help", "path=", "index", "match=", "show-duplicates", "match-map="])
	except getopt.GetoptError:
		usage()
		sys.exit(2)

	for opt, arg in opts:
		if opt in ("-h", "--help"):
			usage()
			sys.exit()
		elif opt in ("-p", "--path"):
			repopath = arg
		elif opt in ("-i", "--index"):
			doindex = True
		elif opt in ("-m", "--match"):
			domatchpath = arg
		elif opt in ("--match-map"):
			matchmappath = arg
		elif opt in ("-d"):
			domatchdelete = True
		elif opt in ("--show-duplicates"):
			doshowduplicates = True

	if repopath is None:
		print "you need to set -p option"
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
	if domatchpath:
		matchcontext = MatchContext(domatchpath, domatchdelete, matchmappath)
		if not os.path.isdir(domatchpath):
			print "not a dir %s" % (domatchpath)
			sys.exit(2)
		print "Merging %s" % (domatchpath)
		repo.match(matchcontext)
	if doshowduplicates:
		print "Show duplicates %s" % (realrepopath)
		repo.show_duplicates(matchmappath, domatchdelete)

if __name__ == "__main__":
    main(sys.argv[1:])