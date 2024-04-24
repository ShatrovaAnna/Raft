import argparse

from inner import ProcessedElectionResults, CommandLineArguments
from transport import ElectionInput


def process_election_results(results: list[dict]):
    processed = ProcessedElectionResults()
    for res in results:
        if 'error' in res:
            print(res['error'])
            continue
        response = res['result']
        if response['vote_granted']:
            processed.positive_responses += 1
        if response['term'] > processed.max_term:
            processed.max_term = response['term']
    return processed


def is_uptodate(inp: ElectionInput, last_term: int, last_log_index: int):
    if inp.last_log_term > last_term:
        return True
    if inp.last_log_term == last_term and inp.last_log_index >= last_log_index:
        return True
    return False


def get_cla():
    parser = argparse.ArgumentParser()
    parser.add_argument('--self-ip')
    parser.add_argument('--self-port', type=int)
    parser.add_argument('--other-ports', nargs="+", type=int)
    args = parser.parse_args()
    return CommandLineArguments(ip=args.self_ip, port=args.self_port, other_ports=args.other_ports)
