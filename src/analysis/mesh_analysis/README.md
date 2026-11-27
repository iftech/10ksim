# Structure (WIP)

![Module Architecture](images/diagram.png "Overview of the Mesh Analysis Module")

This module is in charge of analyzing the logs of the nodes.
The idea is that we have several layers of abstraction, maintaining the structure extensible and modular. We will decide:
1. What Stack we are using (Vaclab, Infra, ...).
2. What kind of analysis we use (Waku, Nimlibp2p, Momos, ...).
3. How we retrieve the log lines (VictoriaLogs, Kibana, ...).
4. Which kind of log lines are we interested in.

## Necessary information

In order to use this, we need information beforehand.
In the case of Waku, what stack is being used is selected in the variable `type`. This will be dispatched in 
WakuAnalyzer automatically. The stack arguments will be sent as kwargs to the inner classes. This should be handled
automatically. For example, as we are using victoria in this example, we know we need the start and end time in a 
specific format, and the extra fields we can get. Also, as we are using Vaclab, so we know we are using stateful sets, 
and we know how many nodes there are per statefulset. 
```
if __name__ == '__main__':
    stack = {'type': 'vaclab',
             'url': 'https://vmselect.riff.cc/select/logsql/query',
             'start_time': '2025-05-26T13:10:00',
             'end_time': '2025-05-26T13:15:00',
             'reader': 'victoria',
             'stateful_sets': ['nodes-0'],
             'nodes_per_statefulset': [50],
             'container_name': 'waku',
             'extra_fields': ['kubernetes.pod_name', 'kubernetes.pod_node_name']
             }
    log_analyzer = WakuAnalyzer(dump_analysis_dir='local_data/simulations_data/refactor/',
                                # local_folder_to_analyze='local_data/simulations_data/waku_simu3/log/',
                                **stack)

    log_analyzer.analyze_reliability(n_jobs=4)
    log_analyzer.check_store_messages()
    log_analyzer.check_filter_messages()
```

## Expand

If you plan to add any functionality to this, in order to maintain the code decoupled, the responsibilities are:
- `Stacks`: This module allows us to do high level operations. Obtain the data we want as csv files, retrieve logs 
and so on. How the data is obtained, must be implemented in the subclasses.
  - `VaclabStack`: Lab currently used by the DST team. It uses `VictoriaLogs`.
- `Readers`: This module is in charge of how the data is retrieved by the Stack.
- `Analyzers`: Module to decide how the analysis of the data needs to be performed. It obviously depends on the project.

