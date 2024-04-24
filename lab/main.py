from contextlib import asynccontextmanager

import fastapi_jsonrpc as jsonrpc
import uvicorn

from inner import RaftRole
from rpc_management import RpcConnectionManager
from storage import RaftDataStorage
from fastapi import Request, Body
from fastapi.responses import JSONResponse
import util
from log import LogEntryDecoder
from commands import HashtablePost
from state_machine import HashTableStateMachine
from transport import ElectionInput, ElectionOutput, AppendInput, AppendOutput, HashtablePostInput

API_ENDPOINT_PATH = "/api/v1/jsonrpc"
api_v1 = jsonrpc.Entrypoint(API_ENDPOINT_PATH)


class MyJSONRPCMethods:
    @staticmethod
    @api_v1.method()
    async def leader_election(input_params: ElectionInput) -> ElectionOutput:
        # Check for falling behind
        if input_params.term > storage.current_term:
            storage.role = RaftRole.FOLLOWER
            storage.set_term(input_params.term)

        # Followers are not allowed to start their own elections if there is a candidate currently
        if storage.role == RaftRole.FOLLOWER:
            storage.restart_timer()

        # Main step - vote if term is adequate, not voted for anyone else in current term and leader's log is up-to-date
        if input_params.term < storage.current_term:
            vote_granted = False
        elif storage.voted_for in (None, input_params.candidate_id) and \
                util.is_uptodate(input_params, storage.get_last_log_term(), len(storage.log) - 1):
            vote_granted = True
            storage.voted_for = input_params.candidate_id
        else:
            vote_granted = False
        return ElectionOutput(term=storage.current_term, vote_granted=vote_granted)

    @staticmethod
    @api_v1.method()
    async def append_entries(input_params: AppendInput) -> AppendOutput:
        print("In append entries: ", str(input_params))
        # Check for falling behind
        if input_params.term > storage.current_term:
            storage.role = RaftRole.FOLLOWER
            storage.set_term(input_params.term)
        if storage.role == RaftRole.CANDIDATE:
            storage.role = RaftRole.FOLLOWER
        input_params.entries = LogEntryDecoder.decode_my_class(input_params.entries)
        storage.cancel_election()
        
        storage.restart_timer()

        storage.leader_id = input_params.leader_id

        output = AppendOutput(term=storage.current_term, success=True)

        # If leader's term is outdated or inconsistent logs, reply with False right away
        if input_params.term < storage.current_term and input_params.term != -1 or \
                len(storage.log) <= input_params.prev_log_index or \
                input_params.prev_log_index != -1 \
                and storage.log[input_params.prev_log_index].term != input_params.prev_log_term:
            output.success = False
            return output

        starting_index = input_params.prev_log_index + 1


        storage.log = storage.log[:starting_index] + input_params.entries

        if input_params.leader_commit > storage.commit_index:
            storage.set_commit_index_and_apply(min(input_params.leader_commit, len(storage.log) - 1))
            print("In append entities in storage state", storage.state_machine.state)
        return output


async def post(request: Request, data: HashtablePostInput):
    if storage.role != RaftRole.LEADER:
        return JSONResponse(content={"error": "Non-leader node"}, status_code=307,
                            headers={"Location": "http:/" + str(storage.ip) + ":" + str(storage.leader_id) + "/query"})
    index_of_log = storage.add_to_log(HashtablePost(data.key, data.value))
    while storage.last_applied < index_of_log:
        await storage.applied_event.wait()
    return JSONResponse(content={"leader_log_index": index_of_log})


async def get(request: Request, key: str = Body(...)):
    if storage.role != RaftRole.LEADER:
        return JSONResponse(content={"error": "Non-leader node"}, status_code=307,
                            headers={"Location": "http:/" + str(storage.ip) + ":" + str(storage.leader_id) + "/query"})
    res = ""
    try:
        if isinstance(storage.state_machine, HashTableStateMachine):
            res = storage.state_machine.get(key)
    except KeyError as e:
        return JSONResponse(content={"error": str(e)}, status_code=404)
    except ValueError as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
    return JSONResponse(content=res)


if __name__ == '__main__':
    # Getting config from command line, setting up
    config = util.get_cla()
    rpc_manager = RpcConnectionManager(config, API_ENDPOINT_PATH)
    storage = RaftDataStorage(config, 10, rpc_manager, HashTableStateMachine(100))

    # Thing that starts the election/heartbeat timer inside storage instance on launch
    @asynccontextmanager
    async def lifespan(app: jsonrpc.API):
        storage.start_timer()
        yield


    # Creating node image, binding endpoints
    app = jsonrpc.API(lifespan=lifespan)

    app.bind_entrypoint(api_v1)
    app.add_api_route("/query", post, methods=["POST"])
    app.add_api_route("/query", get, methods=["GET"])

    # Run the app on web-server
    uvicorn.run(app, host=storage.ip, port=storage.id)

#python main.py --self-ip 127.0.0.1 --self-port 9300 --other-ports 9301