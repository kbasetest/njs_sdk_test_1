#BEGIN_HEADER
# The header block is where all import statments should live
import os
from pprint import pformat
from biokbase.workspace.client import Workspace as workspaceService  # @UnresolvedImport @IgnorePep8
from njs_sdk_test_1.GenericClient import GenericClient
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
        self.generic_clientURL = os.environ['SDK_CALLBACK_URL']
        print('Callback URL: ' + self.generic_clientURL)
        #END_CONSTRUCTOR
        pass

    def run(self, ctx, params):
        # ctx is the context object
        # return variables are: results
        #BEGIN run
        mod = self.__class__.__name__
        print('Running module {} commit {}'.format(mod, self.GIT_COMMIT_HASH))
        token = ctx['token']
        gc = GenericClient(self.generic_clientURL, use_url_lookup=False,
                           token=token)
        calls = {}
        for action in params['actions']:
            del action  # TODO
        if 'save' in params:
            # 1: workspace name
            # 2: workspace object ID
            o = {'name': mod,
                 'hash': self.GIT_COMMIT_HASH,
                 'calls': calls
                 }
            prov = gc.sync_call("CallbackServer.get_provenance", [])
            print('Saving workspace object\n' + pformat(o))
            print('with provenance\n' + pformat(prov))

            ws = workspaceService(self.workspaceURL, token=token)
            print('result:')
            print(ws.save_objects({
                'workspace': params['save']['ws'],
                'objects': [
                    {
                     'type': 'Empty.AType',
                     'data': o,
                     'name': params['save']['name'],
                     'provenance': prov
                     }
                    ]
            }))
        results = {'name': mod,
                   'hash': self.GIT_COMMIT_HASH,
                   'calls': calls}
        #END run

        # At some point might do deeper type checking...
        if not isinstance(results, object):
            raise ValueError('Method run return value ' +
                             'results is not type object as required.')
        # return the results
        return [results]

    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH}
        del ctx  # shut up pep8
        #END_STATUS
        return [returnVal]
