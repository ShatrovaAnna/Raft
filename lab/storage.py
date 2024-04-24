import warnings
from asyncio import Event
import random
from typing import List, Dict

from inner import RaftRole, CommandLineArguments
from rpc_management import RpcConnectionManager
import asyncio

import util
from log import LogEntry
from commands import StateMachineCommand
from state_machine import StateMachine
from transport import AppendInput, ElectionInput


class RaftDataStorage:
    def __init__(self, config: CommandLineArguments, election_timeout,
                 rpc_manager: RpcConnectionManager, state_machine: StateMachine):
        self.state_machine = state_machine
        self.rpc_manager = rpc_manager
        self.ip = config.ip
        self.id = config.port
        self.other_ids = config.other_ports
        self.leader_id = None

        self.role = RaftRole.FOLLOWER
        self.election_timeout = election_timeout
        self.election_timer_task = None
        self.election_task = None
        self.leader_task = None
        #queue for later
        self.back_burner: List[asyncio.Task] = []

        self.current_term = 0
        self.voted_for = None
        self.log: List[LogEntry] = []

        self.commit_index = -1
        self.applied_event = Event()
        self.last_applied = -1

        self.next_index = dict.fromkeys(config.other_ports)
        #approwed
        self.match_index = dict.fromkeys(config.other_ports)

    def get_last_log_term(self):
        if len(self.log) == 0:
            return -1
        else:
            return self.log[len(self.log) - 1].term

    def apply_to_machine(self, command: StateMachineCommand):
        command.apply(self.state_machine)

    def add_to_log(self, command: StateMachineCommand):
        self.log.append(LogEntry(term=self.current_term, command=command))
        return len(self.log) - 1

    async def __get_timer_procedure__(self):
        await asyncio.sleep(
            self.election_timeout / 2 if self.role == RaftRole.LEADER else self.__get_slightly_random_timeout__())
        print("Timer passed, current role - " + str(self.role) + ", term " + str(self.current_term))
        if self.role in [RaftRole.FOLLOWER, RaftRole.CANDIDATE]:
            #stop election if timeout and election is start but not finished
            if not (self.election_task is None or self.election_task.done):
                self.election_task.cancel()
            self.election_task = asyncio.create_task(self.__get_election_procedure__())
            #wait finish of election - защитили таску от отмены
            await asyncio.shield(self.election_task)
        else:
            #start but not finished
            if not (self.leader_task is None or self.leader_task.done):
                print("back-burning " + self.leader_task)
                self.back_burner.append(self.leader_task)
                self.__clean_back_burner__()
            self.leader_task = asyncio.create_task(self.__leader_behaviour__())
            try:
                await asyncio.shield(self.leader_task)
            except Exception as e:
                raise e

    async def __get_election_procedure__(self):
        if self.role == RaftRole.FOLLOWER:
            self.role = RaftRole.CANDIDATE
        if self.role == RaftRole.CANDIDATE:
            self.current_term += 1
            self.voted_for = self.id
            self.restart_timer()
            #кто-то другой начнет присылать, пока ждем. И роль в ассинхронщине скинется
            params_for_rpc = ElectionInput(term=self.current_term, candidate_id=self.id,
                                           last_log_index=len(self.log) - 1,
                                           last_log_term=self.get_last_log_term())
            results = await self.rpc_manager.chu_de_tenko("leader_election", params_for_rpc)
            processed = util.process_election_results(results)
            if processed.max_term > self.current_term:
                self.role = RaftRole.FOLLOWER
                self.set_term(processed.max_term)
                return
            if processed.positive_responses + 1 >= (len(self.other_ids) + 1) / 2:
                self.role = RaftRole.LEADER
                print("Elected as new leader")
                for _id in self.other_ids:
                    self.next_index[_id] = len(self.log)
                    self.match_index[_id] = -1
                self.restart_timer()
                #фоловер скинет в другой поток, смотреть, где меняются state - роли в разных потоках меняется
                heartbeat_input = AppendInput(term=self.current_term, leader_id=self.id,
                                              leader_commit=self.commit_index, prev_log_index=len(self.log) - 1,
                                              prev_log_term=self.get_last_log_term(), entries=[])
                await self.rpc_manager.chu_de_tenko("append_entries", heartbeat_input)

                print("Leader init complete")
            else:
                print("Haven't won election")

    async def __leader_behaviour__(self):
        if not self.role == RaftRole.LEADER:
            raise ValueError("Non-leader doing leader stuff")

        self.restart_timer()

        # Sending every following server its own input, if entry didn't send, add them into new request
        to_send: Dict[int, AppendInput] = dict()
        for _id in self.other_ids:
            if len(self.log) - 1 >= self.next_index[_id]:
                request = AppendInput(term=self.current_term, leader_id=self.id, leader_commit=self.commit_index,
                                      prev_log_index=self.next_index[_id] - 1,
                                      prev_log_term=self.log[self.next_index[_id] - 1].term if self.next_index[_id] > 0
                                      else -1,
                                      entries=self.log[self.next_index[_id]:len(self.log)])
            else:
                request = AppendInput(term=self.current_term, leader_id=self.id, leader_commit=self.commit_index,
                                      prev_log_index=len(self.log) - 1,
                                      prev_log_term=self.get_last_log_term(),
                                      entries=[])
            to_send.__setitem__(_id, request)
        sending_last_index = len(self.log) - 1
        results = await self.rpc_manager.call_with_varied_inputs("append_entries", to_send)

        # Retrying until success (or until too many tasks of such type are produced and this one gets blasted)
        consistency_flag = False
        while not consistency_flag:
            to_send: Dict[int, AppendInput] = dict()
            for _id in results.keys():
                consistency_flag = True
                task_res = results[_id].result()  # Single Task's result as json-like dict
                if "error" in task_res:
                    continue
                task_res = task_res["result"]  # Actual response entity
                if task_res["success"]:
                    self.next_index[_id] = sending_last_index + 1
                    self.match_index[_id] = sending_last_index
                # Request can only be unsuccessful due to 2 things: leader being outdated and log inconsistency;
                # First has more priority
                elif task_res["term"] > self.current_term:
                    self.role = RaftRole.FOLLOWER
                    self.set_term(task_res["term"])
                    return
                else:  # Log inconsistency
                    print("Log inconsistent")
                    consistency_flag = False
                    self.next_index[_id] -= 1
                    to_send.__setitem__(_id, AppendInput(
                        term=self.current_term, leader_id=self.id, leader_commit=self.commit_index,
                        prev_log_index=self.next_index[_id] - 1,
                        prev_log_term=(self.log[self.next_index[_id] - 1].term if self.next_index[_id] > 0 else -1),
                        entries=self.log[self.next_index[_id]:len(self.log)]
                    ))
            results = await self.rpc_manager.call_with_varied_inputs("append_entries", to_send)

        # Checking if new entries can be committed and subsequently applied to own state machine, server approwed commit
        for N in range(self.commit_index + 1, len(self.log)):
            if sum(1 for match_index in self.match_index if match_index >= N) > len(self.match_index) / 2 and \
                    self.log[N].term == self.current_term:
                self.set_commit_index_and_apply(N)
                print("State machine: ", self.state_machine.state)

    def start_timer(self):
        self.election_timer_task = asyncio.create_task(self.__get_timer_procedure__())

    def stop_timer(self):
        self.election_timer_task.cancel()

    def restart_timer(self):
        self.stop_timer()
        self.start_timer()

    def set_term(self, new_term):
        self.current_term = new_term
        self.voted_for = None

    def __get_slightly_random_timeout__(self):
        return random.uniform(self.election_timeout * 0.8, self.election_timeout * 1.2)

    def set_commit_index_and_apply(self, n):
        self.commit_index = n
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            self.apply_to_machine(self.log[self.last_applied].command)
            self.applied_event.set()
            self.applied_event.clear()

    def cancel_election(self):
        if not (self.election_task is None or self.election_task.done):
            self.election_task.cancel()
            self.role = RaftRole.FOLLOWER

    def __clean_back_burner__(self):
        if len(self.back_burner) > 1000:
            warnings.warn("Leader behaviour tasks are getting killed due to overflowed backburner")
            #chose first 250 tasks for cancel
            tasks_to_cancel = self.back_burner[:250]
            for task in tasks_to_cancel:
                task.cancel()
                self.back_burner.remove(task)

        self.back_burner = [task for task in self.back_burner if not task.done()]
