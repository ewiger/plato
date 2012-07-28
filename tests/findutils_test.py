import unittest
import tempfile
import os
import shutil
from plato.findutils import (find_files, Match, collect_size)
from plato.coreutils import touch


class Test(unittest.TestCase):

    def setUp(self):
        # create and populate temporary folder
        self.folder = tempfile.mkdtemp()
        for num in range(10):
            os.mkdir(os.path.join(self.folder, str(num)))
        self.folder_with_files = os.path.join(self.folder, 'with_files')
        os.mkdir(self.folder_with_files)
        for num in range(12):
            touch(os.path.join(self.folder_with_files, str(num)))            
        self.folder_diff_files = os.path.join(self.folder, 'diff_files')
        os.mkdir(self.folder_diff_files)
        for num in range(12):
            filename = os.path.join(self.folder_diff_files, str(num))
            fhandle = open(filename, 'w+')
            fhandle.write(''.join([str(ch) for ch in range(num)]))
            fhandle.close()

    def tearDown(self):
        shutil.rmtree(self.folder)

    def testFind(self):
        # Just search.
        pathnames = [name for name in find_files(self.folder)]        
        self.assertTrue(len(pathnames) > 0)        
        # Search for directories in folder with files only, then search 
        # for files.
        file_search = find_files(self.folder_with_files, Match(filetype='d'))
        pathnames = [name for name in file_search]
        self.assertEquals(len(pathnames), 0)
        file_search = find_files(self.folder_with_files, Match(filetype='f'))
        pathnames = [name for name in file_search]
        self.assertTrue(len(pathnames) > 0)        
        # Search by fnmatch pattern, i.e. wildcard.
        file_search = find_files(
            self.folder_with_files, Match(filetype='f', name='1*'))
        pathnames = [name for name in file_search]        
        self.assertTrue(len(pathnames) > 1)
        condition = lambda pn: os.path.basename(pn).startswith('1')
        self.assertEquals(len(filter(condition, pathnames)), len(pathnames))
    
    def testFindCollect(self):
        # Get files plus their size
        file_search = find_files(
            self.folder_diff_files, 
            Match(filetype='f', name='1*'),
            collect_size,
        )
        for name, size in file_search:
            self.assertTrue(size > 0)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testFind']
    unittest.main()