import os

import psycopg2
from flask import Flask, request

app = Flask(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]


def get_db():
    # very unsafe but cest la vie
    # conn = psycopg2.connect(host="localhost", database="siuvotes2", user="florian", password="postgres")
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    cur = conn.cursor()
    return conn, cur


def close_db(conn, cur, commit=False):
    if commit:
        conn.commit()
    cur.close()
    conn.close()


@app.route("/")
@app.route("/index")
def index():
    name = request.args.get("name", default="", type=str)

    return f"{{'status': 'ok', 'name': {name}}}"


@app.route("/topiclist")
def topiclist():
    try:
        conn, cur = get_db()
        cur.execute("select t.ID, t.TOPIC from topics t ")
        print(cur.query)
        if cur.rowcount < 1:
            close_db(conn, cur)
            return {"error": "No topics found."}

        rows = cur.fetchall()
        close_db(conn, cur)

        return rows

        # return {"yay": 1}

    except Exception as e:
        print(e)
        return {"error": "DB error, see logs"}


@app.route("/login")
def login():
    name = request.args.get("login", default="", type=str)
    if len(name) < 1:
        return {"error": "No authentication provided"}
    try:
        conn, cur = get_db()
        cur.execute(
            "select u.LOGIN, v.URL, r.VOTE, r.ID, t.TOPIC "
            "from users u "
            "join reviews r on (u.ID = r.PERSON) "
            "join videos v on (r.VIDEO = v.ID) "
            "join topics t on (r.TOPIC = t.ID) "
            "where u.LOGIN = %s",
            # "where r.ID = 3"
            (name,),
        )
        print(cur.query)
        if cur.rowcount < 1:
            close_db(conn, cur)
            return {"error": "No videos found for this login"}

        rows = cur.fetchall()
        close_db(conn, cur)

        return rows

        # return {"yay": 1}

    except Exception as e:
        print(e)
        return {"error": "DB error, see logs"}


@app.route("/vote")
def vote():
    review_id = request.args.get("review", default=None, type=int)
    vote_val = request.args.get("vote", default=None, type=int)
    print(f"voting on review {review_id} with vote {vote_val}")

    if review_id is None or vote_val is None:
        return {"error": "Vote or review not provided"}
    try:
        conn, cur = get_db()
        cur.execute("update reviews set VOTE = %s where ID = %s", (vote_val, review_id))
        print(cur.query)
        if cur.rowcount != 1:
            close_db(conn, cur)
            return {"error": f"Error during voting {{'r':{review_id}, 'v':{vote_val}}}"}

        close_db(conn, cur, True)

        return {"success": 1}

        # return {"yay": 1}

    except Exception as e:
        print(e)
        return {"error": "DB error, see logs"}


@app.route("/topic")
def topic():
    review_id = request.args.get("review", default=None, type=int)
    topic_id = request.args.get("topic", default=None, type=int)
    print(f"updating topic on review {review_id} with topic {topic_id}")

    if review_id is None or topic_id is None:
        return {"error": "Topic or review not provided"}
    try:
        conn, cur = get_db()
        cur.execute("update reviews set TOPIC = %s where ID = %s", (topic_id, review_id))
        print(cur.query)
        if cur.rowcount != 1:
            close_db(conn, cur)
            return {"error": f"Error during topic update {{'r':{review_id}, 't':{topic_id}}}"}

        close_db(conn, cur, True)

        return {"success": 1}

        # return {"yay": 1}

    except Exception as e:
        print(e)
        return {"error": "DB error, see logs"}
