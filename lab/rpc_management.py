import asyncio
import json
import warnings
from typing import Dict

import requests
from pydantic import BaseModel
from requests import RequestException
from inner import CommandLineArguments
from log import LogEntryEncoder


class _RpcClient:
    def __init__(self, ip: str, port: int, api_endpoint_path):
        self.port = port
        self.url = f"http://{ip}:{port}" + api_endpoint_path

    #dumps - dictionary loc_json_rpc_req to json. .json() - json to python
    async def call_rpc(self, func_name: str, input_model: BaseModel):
        headers = {'content-type': 'application/json'}
        loc_json_rpc_req = {
            "jsonrpc": "2.0",
            "id": "0",
            "method": func_name,
            "params": {"input_params": input_model.model_dump()}
        }
        try:
            response = requests.post(self.url, data=json.dumps(loc_json_rpc_req, cls=LogEntryEncoder), headers=headers,
                                     timeout=0.5)
        except RequestException:
            return {'error': 'error connection ' + str(self.port)}

        if response.status_code == 200:
            response_data = response.json()
            return response_data
        else:
            return {'error': 'error response ' + str(self.port)}


class RpcConnectionManager:
    def __init__(self, config: CommandLineArguments, api_endpoint_path):
        self.rpc_clients: Dict[int, _RpcClient] = dict()
        for i in config.other_ports:
            self.rpc_clients.__setitem__(i, _RpcClient(config.ip, i, api_endpoint_path))
    
    #roll call, create task for each rpc_clients.values(), each task call call_rpc for each client
    #waits for all tasks to complete and return results
    async def chu_de_tenko(self, method_name: str, request_params: BaseModel):
        tasks = [asyncio.create_task(client.call_rpc(method_name, request_params))
                 for client in self.rpc_clients.values()]
        return await asyncio.gather(*tasks)

    #run for all keys, check that ley is real
    #in the end return results and exceptions
    async def call_with_varied_inputs(self, method_name: str, request_params: Dict[int, BaseModel]) \
            -> Dict[int, asyncio.Task]:
        tasks = dict()
        for _id in request_params.keys():
            if _id in self.rpc_clients.keys():
                tasks.__setitem__(_id,
                                  asyncio.create_task(self.rpc_clients[_id].call_rpc(method_name, request_params[_id])))
            else:
                warnings.warn("Manager lacks ID provided as input; manager IDs - " + str(self.rpc_clients.keys()) +
                              ", ID in input - " + str(_id))
        try:
            await asyncio.gather(*tasks.values(), return_exceptions=True)
        except Exception as e:
            raise e
        return tasks
