# Usage:
#     cd noisepage
#     python3 -m script.testing.basic.database

from ..util.db_server import NoisePageServer

if __name__ == "__main__":
    server = NoisePageServer(host="noisepage")

    # run db
    server.run_db()

    # create database
    server.execute("CREATE DATABASE test;", expect_result=False, quiet=False)

    # # delete database
    # server.execute("DELETE DATABASE test;", expect_result=False, quiet=False)

    # stop db
    server.stop_db()
