import json
import time
class Pipeline:
    def __init__(self, client):
        self.cl = client

    def parentNode_isFinished(self, *args) -> bool:
        """
        Check if the parent node is finished at this instance. Return
        appropriate bool.

        Returns:
            bool: is parent done? True or False

        """
        d_parent: dict = None
        b_finished: bool = False
        if len(args): d_parent = args[0]
        if not d_parent:
            b_finished = True
        else:
            print(d_parent)
            b_finished = d_parent['finished']
        return b_finished

    def pluginInstanceID_findWithTitle(self,
                                       d_workflowDetail: dict,
                                       node_title: str
                                       ) -> int:
        """
        Determine the plugin instance id in the `d_workflowDetail` that has
        title substring <node_title>. If the d_workflowDetail is simply a plugin
        instance, return its id provided it has the <node_title>.

        Args:
            d_workflowDetail (dict):    workflow detail data structure
            node_title (str):           the node to find

        Returns:
            int: id of the found node, or -1
        """

        def plugin_hasTitle(d_plinfo: dict, title: str) -> bool:
            """
            Does this node (`d_plinfo`) have this `title`?

            Args:
                d_plinfo (dict): the plugin data description
                title (str):     the name of this node

            Returns:
                bool: yay or nay
            """

            nonlocal pluginIDwithTitle
            if title.lower() in d_plinfo['title'].lower():
                pluginIDwithTitle = d_plinfo['id']
                return True
            else:
                return False

        pluginIDwithTitle: int = -1
        d_plinfo: dict = {}
        if 'data' in d_workflowDetail:
            for d_plinfo in d_workflowDetail['data']:
                if plugin_hasTitle(d_plinfo, node_title): break
        else:
            plugin_hasTitle(d_workflowDetail, node_title)

        return pluginIDwithTitle

    def waitForNodeInWorkflow(self,
                              d_workflowDetail: dict,
                              node_title: str,
                              **kwargs
                              ) -> dict:
        """
        Wait for a node in a workflow to transition to a finishedState

        Args:
            d_workflowDetail (dict): the workflow in which the node
                                     exists
            node_title (str):        the title of the node to find

        kwargs:
                waitPoll        = the polling interval in seconds
                totalPolls      = total number of polling before abandoning;
                                  if this is 0, then poll forever.

        Future: expand to wait on list of node_titles

        Returns:
            dict: _description_
        """
        waitPoll: int = 5
        totalPolls: int = 100
        pollCount: int = 0
        b_finished: bool = False
        waitOnPluginID: int = self.pluginInstanceID_findWithTitle(
            d_workflowDetail, node_title
        )
        str_pluginStatus: str = 'unknown'
        d_plinfo: dict = {}

        for k, v in kwargs.items():
            if k == 'waitPoll':     waitPoll = v
            if k == 'totalPolls':   totalPolls = v

        if waitOnPluginID >= 0:
            while 'finished' not in str_pluginStatus.lower() and \
                'cancelled' not in str_pluginStatus.lower() and \
                    pollCount <= totalPolls:
                d_plinfo = self.cl.get_plugin_instance_by_id(waitOnPluginID)
                str_pluginStatus = d_plinfo['status']
                time.sleep(waitPoll)
                if totalPolls:  pollCount += 1
            if 'finished' in d_plinfo['status']:
                b_finished = d_plinfo['status'] == 'finishedSuccessfully'
        return {
            'finished': b_finished,
            'status': str_pluginStatus,
            'workflow': d_workflowDetail,
            'plinst': d_plinfo,
            'polls': pollCount,
            'plid': waitOnPluginID
        }

    def pluginParameters_setInNodes(self,
                                    d_piping: dict,
                                    d_pluginParameters: dict
                                    ) -> dict:
        """
        Override default parameters in the `d_piping`

        Args:
            d_piping (dict):            the current default parameters for the
                                        plugins in a pipeline
            d_pluginParameters (dict):  a list of plugins and parameters to
                                        set in the response

        Returns:
            dict:   a new piping structure with changes to some parameter values
                    if required. If no d_pluginParameters is passed, simply
                    return the piping unchanged.

        """
        for pluginTitle, d_parameters in d_pluginParameters.items():
            for piping in d_piping:
                if pluginTitle in piping.get('title'):
                    for k, v in d_parameters.items():
                        for d_default in piping.get('plugin_parameter_defaults'):
                            if k in d_default.get('name'):
                                d_default['default'] = v
        return d_piping

    def pipelineWithName_getNodes(
            self,
            str_pipelineName: str,
            d_pluginParameters: dict = {}
    ) -> dict:
        """
        Find a pipeline that contains the passed name <str_pipelineName>
        and if found, return a nodes dictionary. Optionally set relevant
        plugin parameters to values described in <d_pluginParameters>


        Args:
            str_pipelineName (str):         the name of the pipeline to find
            d_pluginParameters (dict):      a set of optional plugin parameter
                                            overrides

        Returns:
            dict: node dictionary (name, compute env, default parameters)
                  and id of the pipeline
        """
        # pudb.set_trace()
        id_pipeline: int = -1
        ld_node: list = []
        d_pipeline: dict = self.cl.get_pipelines({'name': str_pipelineName})
        if 'data' in d_pipeline:
            id_pipeline: int = d_pipeline['data'][0]['id']
            d_response: dict = self.cl.get_pipeline_default_parameters(
                id_pipeline, {'limit': 1000}
            )
            if 'data' in d_response:
                ld_node = self.pluginParameters_setInNodes(
                    self.cl.compute_workflow_nodes_info(d_response['data'], True),
                    d_pluginParameters)
                for piping in ld_node:
                    if piping.get('compute_resource_name'):
                        del piping['compute_resource_name']
        return {
            'nodes': ld_node,
            'id': id_pipeline
        }

    def workflow_schedule(self,
                          inputDataNodeID: str,
                          str_pipelineName: str,
                          d_pluginParameters: dict = {}
                          ) -> dict:
        """
        Schedule a workflow that has name <str_pipelineName> off a given node id
        of <inputDataNodeID>.

        Args:
            inputDataNodeID (str):      id of parent node
            str_pipelineName (str):     substring of workflow name to connect
            d_pluginParameters (dict):  optional structure of default parameter
                                        overrides

        Returns:
            dict: result from calling the client `get_workflow_plugin_instances`
        """
        d_pipeline: dict = self.pipelineWithName_getNodes(
            str_pipelineName, d_pluginParameters
        )
        d_workflow: dict = self.cl.create_workflow(
            d_pipeline['id'],
            {
                'previous_plugin_inst_id': inputDataNodeID,
                'nodes_info': json.dumps(d_pipeline['nodes'])
            })
        d_workflowInst: dict = self.cl.get_workflow_plugin_instances(
            d_workflow['id'], {'limit': 1000}
        )
        return d_workflowInst

    def parentNode_IDget(self, *args) -> int:
        """

                Simply get the plugin instance of the passed parent node

        Returns:
            int: parent plugin instance id of passed `d_parent` structure
        """
        id: int = -1
        d_parent: dict = None
        if len(args):
            d_parent = args[0]
            id = d_parent['plinst']['id']
        return id

    def flow_executeAndBlockUntilNodeComplete(
            self,
            *args,
            **kwargs,
    ) -> dict:
        """
        Execute a workflow identified by a (sub string) in its
        <str_workflowTitle> by anchoring it to <attachToNodeID> in the
        feed/compute tree. This <attachToNodeID> can be supplied in the
        kwargs, or if omitted, then the "parent" node passed in args[0]
        is assumed to be the connector.

        Once attached to a node, the whole workflow is scheduled. This
        workflow will have N>=1 compute nodes, each identified by a
        title. This method will only "return" to a caller when one of
        these nodes with 'waitForNodeWithTitle' enters the finished
        state. Note that this state can be 'finishedSuccessfully' or
        'finishedWithError'.

        Possible future extension: block until node _list_ complete
        """
        d_prior: dict = None
        str_workflowTitle: str = "no workflow title"
        attachToNodeID: int = -1
        str_blockNodeTitle: str = "no node title"
        b_canFlow: bool = False
        d_pluginParameters: dict = {}
        d_ret: dict = {}

        for k, v in kwargs.items():
            if k == 'workflowTitle':   str_workflowTitle = v
            if k == 'attachToNodeID':   attachToNodeID = v
            if k == 'waitForNodeWithTitle':   str_blockNodeTitle = v
            if k == 'pluginParameters':   d_pluginParameters = v

        if self.parentNode_isFinished(*args):
            if attachToNodeID == -1:
                attachToNodeID = self.parentNode_IDget(*args)
            d_ret = self.waitForNodeInWorkflow(
                self.workflow_schedule(
                    attachToNodeID,
                    str_workflowTitle,
                    d_pluginParameters
                ),
                str_blockNodeTitle,
                **kwargs
            )
            if len(args):
                d_ret['prior'] = args[0]
            else:
                d_ret['prior'] = None
        return d_ret