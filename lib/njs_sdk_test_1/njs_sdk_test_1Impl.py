#BEGIN_HEADER
# The header block is where all import statments should live
import os
from pprint import pformat
from biokbase.workspace.client import Workspace as workspaceService  # @UnresolvedImport @IgnorePep8
from njs_sdk_test_1.GenericClient import GenericClient
import time
from multiprocessing.pool import ThreadPool
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
    def log(self, message, prefix_newline=False):
        mod = self.__class__.__name__
        print(('\n' if prefix_newline else '') +
              str(time.time()) + ' ' + mod + ': ' + str(message))
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self.workspaceURL = config['workspace-url']
        self.generic_clientURL = os.environ['SDK_CALLBACK_URL']
        self.log('Callback URL: ' + self.generic_clientURL)
        #END_CONSTRUCTOR
        pass

    def run(self, ctx, params):
        # ctx is the context object
        # return variables are: results
        #BEGIN run
        mod = self.__class__.__name__
        self.log('Running commit {} with params:\n{}'.format(
            self.GIT_COMMIT_HASH, pformat(params)))
        token = ctx['token']

        calls = []
        async = []

        if 'calls' in params:
            gc = GenericClient(self.generic_clientURL, use_url_lookup=False,
                               token=token)
            for c in params['calls']:
                meth = c['method']
                par = c['params']
                ver = c['ver']
                self.log(('Synchronously calling method {} version {} with ' +
                         'params:\n{}').format(meth, ver, pformat(par)))
                calls.append(gc.sync_call(
                    meth, par, json_rpc_context={'service_ver': ver}))
        if 'async_jobs' in params:
            wait_time = params.get('async_wait')
            if not wait_time:
                wait_time = 10000
            gc = GenericClient(self.generic_clientURL, use_url_lookup=False,
                               token=token, async_job_check_time_ms=wait_time)

            # jobs must be a list of lists, each sublist is
            # [module.method, [params], service_ver]
            jobs = params['async_jobs']
            self.log('Running jobs asynchronously:')
            for j in jobs:
                self.log('Method: {} version: {} params:\n{}'.format(
                    j[0], j[2], pformat(j[1])))
            pool = ThreadPool(processes=len(jobs))
            async = pool.map(gc.asynchronous_call, jobs, chunksize=1)

        if 'wait' in params:
            self.log('waiting for ' + str(params['wait'] + ' ms'))
            time.sleep(params['wait'])
        if 'save' in params:
            # 1: workspace name
            # 2: workspace object ID
            o = {'name': mod,
                 'hash': self.GIT_COMMIT_HASH,
                 'calls': calls
                 }
            gc = GenericClient(self.generic_clientURL, use_url_lookup=False,
                               token=token)
            prov = gc.sync_call("CallbackServer.get_provenance", [])[0]
            self.log('Saving workspace object\n' + pformat(o))
            self.log('with provenance\n' + pformat(prov))

            ws = workspaceService(self.workspaceURL, token=token)
            info = ws.save_objects({
                'workspace': params['save']['ws'],
                'objects': [
                    {
                     'type': 'Empty.AType',
                     'data': o,
                     'name': params['save']['name'],
                     'provenance': prov
                     }
                    ]
            })
            self.log('result:')
            self.log(info)
        results = {'name': mod,
                   'hash': self.GIT_COMMIT_HASH}
        if calls:
            results['calls'] = calls
        if async:
            results['async'] = async
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
