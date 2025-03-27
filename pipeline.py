import json
class Pipeline:
    def __init__(self, client):
        self.cl = client

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