'''
Created on Jun 29, 2012

@author: yy
'''
import unittest
from plato.metashell import (Pipeline, Program,  Command)
import logging

#logging.basicConfig(level=logging.DEBUG)

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testCommand(self):
        command = Command(('some', 'command'))
        self.assertEquals('some command', command.compose())
        
    def testProgram(self):
        program = Program('echo', '"hello world"')
        self.assertEquals('echo "hello world"', program.compose())
        program = Program('echo', 'hello world')
        self.assertEquals('echo "hello world"', program.compose())
        #program = Program('echo', 'hello world', shell='/bin/bash')        
        program = Program('echo', 'hello world')
        expected_result = 'hello world\n'
        observed_result = program.run()
        self.assertEquals(expected_result, observed_result)
        # Just check if compose does not mess with order of tokens.
        expected_result = 'echo "hello world"'
        observed_result = program.compose()
        self.assertEquals(expected_result, observed_result)

    def testPipeline(self):        
        pipe = Pipeline(Program('echo','hello world'), Program('wc'))
        expected_result = 'echo "hello world" | wc'
        observed_result = pipe.compose()
        self.assertEquals(expected_result, observed_result)
        expected_result = '      1       2      12\n'
        observed_result = pipe.run()
        self.assertEquals(expected_result, observed_result)
        
    def testOperatorOverload(self):
        command1 = Command(('echo','and', 'second', 'command'))
        command2 = Command(('xargs','echo','first', 'command'))
        pipe = command1 | command2        
        expected_result = 'first command and second command\n'
        observed_result = pipe.run()
        self.assertEquals(expected_result, observed_result)
        pipe = pipe | Command(('xargs','echo','prefix of'))
        self.assertTrue(all([type(token) == Command for token in pipe.tokens]))
        expected_result = 'prefix of first command and second command\n'
        observed_result = pipe.run()
        self.assertEquals(expected_result, observed_result)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testPipeline']
    unittest.main()