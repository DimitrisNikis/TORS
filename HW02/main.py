import logging
import random
import sys
import threading
import time
from uuid import uuid4

import requests
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse


def cas_get(data, key, lock):
    with lock:
        return data.get(key, None)


def cas_set(data, key, value, lock):
    with lock:
        current_value = data.get(key, None)
        if current_value:
            return False
        data[key] = value
        return True


def cas_update(data, key, expected_value, new_value, lock):
    with lock:
        current_value = data.get(key, None)
        if current_value and current_value == expected_value:
            data[key] = new_value
            return True
        return False


def cas_pop(data, key, lock):
    with lock:
        return data.pop(key, None)


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

app = FastAPI()

host_name = "http://127.0.0.1:"

state = {
    "role": "replica",  # Роль узла: replica, master или candidate
    "data": dict(),  # Локальное хранилище данных
    "wal": [],  # Write-Ahead Log
    "peers": [
        [f"{host_name}{port}", 1] for port in range(5030, 5033)
    ],  # Список узлов, 1 – узел рабочий, 0 – нет
    "master": None,  # Текущий мастер
    "node_id": str(uuid4()),  # Уникальный идентификатор узла
    "term": 0,  # Текущий Raft term
    "votes": 0,  # Голоса, полученные при выборах
    "last_heartbeat": time.time(),  # Последнее обновление от мастера
    "election_timeout": random.uniform(2, 5),
    "port": None,  # Порт текущего узла
}
db_lock = threading.Lock()


# ====================== CRUD ============================


@app.get("/data/{key}")
async def get_item(key: str):
    if state["role"] == "master":
        alive_peers = [peer for peer in state["peers"] if peer[1] != 0]
        if alive_peers:
            replica = random.choice(alive_peers)
            return Response(content=f"{replica[0]}/data/{key}", status_code=302)
        else:
            value = cas_get(state["data"], key, db_lock)
            entry = {"operation": "post", "key": key, "value": value}
            state["wal"].append(entry)
            return JSONResponse(
                content=cas_get(state["data"], key, db_lock), status_code=200
            )
    else:
        logger.info(f"{state['port']}: key - {key}")
        return JSONResponse(
            content=cas_get(state["data"], key, db_lock), status_code=200
        )


@app.post("/data/")
async def create_item(key: str, value: str):
    if state["role"] == "master":
        success = replicate_with_majority(
            {"operation": "post", "key": key, "value": value}
        )
        if success == "exists":
            raise HTTPException(
                status_code=400, detail=f"Key '{key}' already exists, post"
            )
        if success:
            return JSONResponse(content={"status": "success"}, status_code=200)
        else:
            raise HTTPException(status_code=500, detail="Majority not reached, post")
    else:
        raise HTTPException(
            status_code=500, detail="The request was sent not to the master node, post"
        )


@app.put("/data/{key}")
async def update_item(key: str, value: str):
    if state["role"] == "master":
        success = replicate_with_majority(
            {
                "operation": "put",
                "key": key,
                "old_value": cas_get(state["data"], key, db_lock),
                "value": value,
            }
        )
        if success == "not_found":
            raise HTTPException(
                status_code=404, detail=f"Key '{key}' does not exist, put"
            )

        if success:
            return JSONResponse(content={"status": "success"}, status_code=200)
        else:
            raise HTTPException(status_code=500, detail="Majority not reached, put")
    else:
        raise HTTPException(
            status_code=500, detail="The request was sent not to the master node, put"
        )


@app.patch("/data/{key}")
async def partial_update_item(key: str, value: str):
    if state["role"] == "master":
        success = replicate_with_majority(
            {
                "operation": "patch",
                "key": key,
                "old_value": cas_get(state["data"], key, db_lock),
                "value": value,
            }
        )
        if success == "not_found":
            raise HTTPException(
                status_code=404, detail=f"Key '{key}' does not exist, put"
            )
        if success:
            return JSONResponse(content={"status": "success"}, status_code=200)
        else:
            raise HTTPException(status_code=500, detail="Majority not reached, patch")
    else:
        raise HTTPException(
            status_code=500, detail="The request was sent not to the master node, patch"
        )


@app.delete("/data/{key}")
async def delete_item(key: str):
    if state["role"] == "master":
        success = replicate_with_majority({"operation": "delete", "key": key})
        if success:
            return JSONResponse(content={"status": "success"}, status_code=200)
        else:
            raise HTTPException(status_code=500, detail="Majority not reached, delete")
    else:
        raise HTTPException(
            status_code=500,
            detail="The request was sent not to the master node, delete",
        )


def replicate_with_majority(entry):
    check_key_status = True
    state["wal"].append(entry)
    if entry["operation"] != "delete":
        if entry["operation"] != "post":
            check_key_status = cas_update(
                state["data"], entry["key"], entry["old_value"], entry["value"], db_lock
            )
            if not check_key_status:
                return "not_found"
        else:
            check_key_status = cas_set(
                state["data"], entry["key"], entry.get("value"), db_lock
            )
            if not check_key_status:
                return "exists"
    else:
        cas_pop(state["data"], entry["key"], db_lock)

    confirmations = 1
    total_peers = len(state["peers"])
    majority = total_peers // 2

    def replicate_to_peer(node_link):
        nonlocal confirmations
        try:
            response = requests.post(
                f"{node_link}/replicate",
                json=entry,
                headers={"Master-Term": str(state["term"])},
                timeout=2,
            )
            if response.status_code == 200:
                confirmations += 1
        except requests.exceptions.RequestException:
            pass

    if check_key_status:
        threads = []
        for peer in state["peers"]:
            thread = threading.Thread(target=replicate_to_peer, args=(peer[0],))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        return confirmations >= majority
    return check_key_status


@app.post("/replicate")
async def handle_replication(entry: dict, request: Request):
    """Обработка репликации лога от мастера"""
    master_term = int(request.headers.get("Master-Term", -1))

    if master_term < state["term"]:
        logger.warning(
            f"Replication rejected: master's term {master_term} is less than node's term {state['term']}."
        )
        return JSONResponse(
            content={"status": "rejected", "reason": "Term mismatch"},
            status_code=400,
        )

    if master_term > state["term"]:
        state["term"] = master_term

    state["wal"].append(entry)
    if entry["operation"] != "delete":
        if entry["operation"] != "post":
            cas_update(
                state["data"], entry["key"], entry["old_value"], entry["value"], db_lock
            )
        else:
            cas_set(state["data"], entry["key"], entry["value"], db_lock)
    else:
        cas_pop(state["data"], entry["key"], db_lock)

    return JSONResponse(content={"status": "ok"}, status_code=200)


# ====================== Raft ============================


@app.post("/election")
async def handle_election(request: Request):
    """Обработка запроса голоса на выборах"""
    body = await request.json()
    candidate_id = body.get("candidate_id")
    term = body.get("term")

    logger.info(f"{state['port']}: has sent a vote for {candidate_id}")

    if term > state["term"]:
        state["term"] = term
        state["role"] = "replica"
        state["votes"] = 0
        state["last_heartbeat"] = time.time()
        return JSONResponse(content={"vote_granted": True}, status_code=200)
    return JSONResponse(content={"vote_granted": False}, status_code=200)


def start_election():
    """Инициировать выборы мастера"""
    state["role"] = "candidate"
    state["term"] += 1
    state["votes"] = 1
    state["master"] = None

    answers_amount = 0

    def request_vote(node_link):
        nonlocal answers_amount
        try:
            response = requests.post(
                f"{node_link}/election",
                json={"candidate_id": state["node_id"], "term": state["term"]},
                timeout=1,
            )

            if response.status_code == 200:
                answers_amount += 1
                if response.json().get("vote_granted"):
                    state["votes"] += 1
        except requests.exceptions.RequestException:
            logger.info(
                f"{state['port']}: there was problem with sending request_vote to {node_link}"
            )
            pass

    threads = []
    for peer in state["peers"]:
        if peer[1] != 0:
            logger.info(f"{state['port']}: want to send request to {peer}")
            thread = threading.Thread(target=request_vote, args=(peer[0],))
            thread.start()
            threads.append(thread)

    for thread in threads:
        thread.join()

    logger.info(f"{state['port']} votes count: {state['votes']}")

    if state["votes"] >= (answers_amount + 1) // 2 + 1:
        state["votes"] = 0
        state["role"] = "master"
        state["master"] = f"{host_name}{state['port']}"
        logger.info(f"{state['port']} became master")
        threading.Thread(target=send_heartbeat, daemon=True).start()
    else:
        state["votes"] = 0
        state["role"] = "replica"
        logger.info(f"{state['port']} unable to become master")


def monitor_timeouts():
    logger.info(f"!!! {state['port']}: entered cycle of monitor_timeouts")
    while True:
        if (
            state["role"] == "replica"
            and time.time() - state["last_heartbeat"] > state["election_timeout"]
        ):
            logger.info(f"{state['node_id']} time-out – starts election")
            start_election()
        time.sleep(2)


@app.post("/heartbeat")
async def handle_heartbeat(request: Request):
    body = await request.json()
    term = body.get("term")
    master_id = body.get("master_id")
    master_data = body.get("data", {})
    wal_data = body.get("wal_data", {})

    if term >= state["term"]:
        state["term"] = term
        state["master"] = master_id
        state["role"] = "replica"
        state["last_heartbeat"] = time.time()
        state["data"] = master_data
        state["wal"] = wal_data
        return JSONResponse(content={"status": "ok"}, status_code=200)

    return JSONResponse(content={"status": "rejected"}, status_code=400)


def send_heartbeat():
    while state["role"] == "master":
        for peer_id in range(len(state["peers"])):
            try:
                response = requests.post(
                    f"{state['peers'][peer_id][0]}/heartbeat",
                    json={
                        "term": state["term"],
                        "master_id": state["node_id"],
                        "data": state["data"],
                        "wal_data": state["wal"],
                    },
                    timeout=1,
                )

                if response.status_code in [200, 400]:
                    state["peers"][peer_id][1] = 1
                else:
                    state["peers"][peer_id][1] = 0

            except requests.exceptions.RequestException:
                state["peers"][peer_id][1] = 0
                pass

        time.sleep(0.1)


# ====================== Запуск узла ============================

if __name__ == "__main__":
    import sys

    import uvicorn

    port = int(sys.argv[1])
    state["port"] = port
    state["peers"].remove([f"{host_name}{port}", 1])
    logger.info(f"!!! {state['port']}: port started !!!")
    threading.Thread(target=monitor_timeouts, daemon=True).start()
    logger.info(f"!!! {state['port']}: started monitor !!!")

    uvicorn.run(app, host="127.0.0.1", port=port)
