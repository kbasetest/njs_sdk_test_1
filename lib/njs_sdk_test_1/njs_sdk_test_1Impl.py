#BEGIN_HEADER
# The header block is where all import statments should live
import sys
import traceback
import uuid
from pprint import pprint, pformat
from biokbase.workspace.client import Workspace as workspaceService
#END_HEADER


class njs_sdk_test_1:
    '''
    Module Name:
    njs_sdk_test_1

    Module Description:
    A KBase module: njs_sdk_test_1
    '''

    ######## WARNING FOR GEVENT USERS #######
    # Since asynchronous IO can lead to methods - even the same method -
    # interrupting each other, you must be *very* careful when using global
    # state. A method could easily clobber the state set by another while
    # the latter method is running.
    #########################################
    VERSION = "0.0.1"
    GIT_URL = ""
    GIT_COMMIT_HASH = "ef4be0b1bd369ec2d5f0e878015465170c2c9a73"
    
    #BEGIN_CLASS_HEADER
    # Class variables and functions can be defined in this block
    workspaceURL = None
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self.workspaceURL = config['workspace-url']
        #END_CONSTRUCTOR
        pass
    

    def run(self, ctx, params):
        # ctx is the context object
        # return variables are: results
        #BEGIN run
        #END run

        # At some point might do deeper type checking...
        if not isinstance(results, object):
            raise ValueError('Method run return value ' +
                             'results is not type object as required.')
        # return the results
        return [results]

    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK", 'message': "", 'version': self.VERSION, 
                     'git_url': self.GIT_URL, 'git_commit_hash': self.GIT_COMMIT_HASH}
        #END_STATUS
        return [returnVal]
