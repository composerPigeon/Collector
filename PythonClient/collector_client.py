#!/usr/bin/env python3

import argparse
import logging.config
import requests
from requests import HTTPError

import logging

HOST: str = "localhost"
PORT: int = 8080

logging.basicConfig(format="%(message)s")

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="Python client for Wrapper"
    )

    parser.add_argument("--instance", "-i")
    parser.add_argument("--query", "-q")
    parser.add_argument("--state", "-s")
    parser.add_argument("--result", "-r")
    parser.add_argument("--list", "-l", action="store_true")

    return parser

def create_execution(instance: str, query: str) -> str:
    try:
        response = requests.post(
            url = f"http://{HOST}:{PORT}/query",
            json = { "instance": instance, "query": query }
        )
        print(response.content)
        response.raise_for_status()
        return response.text
    except HTTPError as e:
        logging.error(f"Error creating execution: {e}")

def list_wrappers() -> str:
    try:
        response = requests.get(
            url=f"http://{HOST}:{PORT}/instances/list"
        )
        response.raise_for_status()
        return response.text
    except HTTPError as e:
        logging.error(f"Error listing wrappers: {e}")

def get_status(uuid: str) -> str:
    try:
        response = requests.get(
            url=f"http://{HOST}:{PORT}/query/{uuid}/state"
        )
        response.raise_for_status()
        return response.text
    except HTTPError as e:
            logging.error(f"Error getting state of execution {uuid}: {e}")

def get_result(uuid: str) -> str:
    try:
        response = requests.get(
            url=f"http://{HOST}:{PORT}/query/{uuid}/result"
        )
        response.raise_for_status()
        return response.text
    except HTTPError as e:
            logging.error(f"Error getting result of execution {uuid}: {e}")


if __name__ == "__main__":
    parser = init_argparse()
    args: argparse.Namespace = parser.parse_args()

    result: str = ""

    if args.instance != None and args.query != None:
        result = create_execution(args.instance, args.query)
    elif args.state != None:
        result = get_status(args.state)
    elif args.result != None:
        result = get_result(args.result)
    elif args.list:
        result = list_wrappers()
    
    print(result)




