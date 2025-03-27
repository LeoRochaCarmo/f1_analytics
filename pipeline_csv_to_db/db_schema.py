from sqlalchemy import create_engine, text
from pathlib import Path
import pandas as pd

DB_NAME = 'f1_analytics.db'
DB_FOLDER = 'data'
DB_PATH = Path(__file__).parent.joinpath(DB_FOLDER).joinpath(DB_NAME)
engine = create_engine(f'sqlite:///{DB_PATH}')

# SQL Schema
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
"""

# List of CREATE TABLE statements
create_statements = [
    """
    CREATE TABLE seasons (
        year BIGINT PRIMARY KEY, 
        url TEXT
    );
    """,
    """
    CREATE TABLE circuits (
        "circuitId" BIGINT PRIMARY KEY, 
        "circuitRef" TEXT, 
        name TEXT, 
        location TEXT, 
        country TEXT, 
        lat FLOAT, 
        lng FLOAT, 
        alt BIGINT, 
        url TEXT
    );
    """,
    """
    CREATE TABLE constructors (
        "constructorId" BIGINT PRIMARY KEY, 
        "constructorRef" TEXT, 
        name TEXT, 
        nationality TEXT, 
        url TEXT
    );
    """,
    """
    CREATE TABLE drivers (
        "driverId" BIGINT PRIMARY KEY, 
        "driverRef" TEXT, 
        number TEXT, 
        code TEXT, 
        forename TEXT, 
        surname TEXT, 
        dob TEXT, 
        nationality TEXT, 
        url TEXT
    );
    """,
    """
    CREATE TABLE races (
        "raceId" BIGINT PRIMARY KEY, 
        year BIGINT, 
        round BIGINT, 
        "circuitId" BIGINT, 
        name TEXT, 
        date TEXT, 
        time TEXT, 
        url TEXT, 
        fp1_date TEXT, 
        fp1_time TEXT, 
        fp2_date TEXT, 
        fp2_time TEXT, 
        fp3_date TEXT, 
        fp3_time TEXT, 
        quali_date TEXT, 
        quali_time TEXT, 
        sprint_date TEXT, 
        sprint_time TEXT,
        FOREIGN KEY ("circuitId") REFERENCES circuits("circuitId") ON DELETE CASCADE,
        FOREIGN KEY ("year") REFERENCES seasons("year") ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE constructor_results (
        "constructorResultsId" BIGINT PRIMARY KEY, 
        "raceId" BIGINT, 
        "constructorId" BIGINT, 
        points FLOAT, 
        status TEXT,
        FOREIGN KEY ("raceId") REFERENCES races("raceId") ON DELETE CASCADE,
        FOREIGN KEY ("constructorId") REFERENCES constructors("constructorId") ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE constructor_standings (
        "constructorStandingsId" BIGINT PRIMARY KEY, 
        "raceId" BIGINT, 
        "constructorId" BIGINT, 
        points FLOAT, 
        position BIGINT, 
        "positionText" TEXT, 
        wins BIGINT,
        FOREIGN KEY ("raceId") REFERENCES races("raceId") ON DELETE CASCADE,
        FOREIGN KEY ("constructorId") REFERENCES constructors("constructorId") ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE driver_standings (
        "driverStandingsId" BIGINT PRIMARY KEY, 
        "raceId" BIGINT, 
        "driverId" BIGINT, 
        points FLOAT, 
        position BIGINT, 
        "positionText" TEXT, 
        wins BIGINT,
        FOREIGN KEY ("raceId") REFERENCES races("raceId") ON DELETE CASCADE,
        FOREIGN KEY ("driverId") REFERENCES drivers("driverId") ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE lap_times (
        "raceId" BIGINT, 
        "driverId" BIGINT, 
        lap BIGINT, 
        position BIGINT, 
        time TEXT, 
        milliseconds BIGINT,
        PRIMARY KEY ("raceId", "driverId", "lap"),
        FOREIGN KEY ("raceId") REFERENCES races("raceId") ON DELETE CASCADE,
        FOREIGN KEY ("driverId") REFERENCES drivers("driverId") ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE pit_stops (
        "raceId" BIGINT, 
        "driverId" BIGINT, 
        stop BIGINT, 
        lap BIGINT, 
        time TEXT, 
        duration TEXT, 
        milliseconds BIGINT,
        PRIMARY KEY ("raceId", "driverId", "stop"),
        FOREIGN KEY ("raceId") REFERENCES races("raceId") ON DELETE CASCADE,
        FOREIGN KEY ("driverId") REFERENCES drivers("driverId") ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE qualifying (
        "qualifyId" BIGINT PRIMARY KEY, 
        "raceId" BIGINT, 
        "driverId" BIGINT, 
        "constructorId" BIGINT, 
        number BIGINT, 
        position BIGINT, 
        q1 TEXT, 
        q2 TEXT, 
        q3 TEXT,
        FOREIGN KEY ("raceId") REFERENCES races("raceId") ON DELETE CASCADE,
        FOREIGN KEY ("driverId") REFERENCES drivers("driverId") ON DELETE CASCADE,
        FOREIGN KEY ("constructorId") REFERENCES constructors("constructorId") ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE results (
        "resultId" BIGINT PRIMARY KEY, 
        "raceId" BIGINT, 
        "driverId" BIGINT, 
        "constructorId" BIGINT, 
        number TEXT, 
        grid BIGINT, 
        position TEXT, 
        "positionText" TEXT, 
        "positionOrder" BIGINT, 
        points FLOAT, 
        laps BIGINT, 
        time TEXT, 
        milliseconds TEXT, 
        "fastestLap" TEXT, 
        rank TEXT, 
        "fastestLapTime" TEXT, 
        "fastestLapSpeed" TEXT, 
        "statusId" BIGINT,
        FOREIGN KEY ("raceId") REFERENCES races("raceId") ON DELETE CASCADE,
        FOREIGN KEY ("driverId") REFERENCES drivers("driverId") ON DELETE CASCADE,
        FOREIGN KEY ("constructorId") REFERENCES constructors("constructorId") ON DELETE CASCADE,
        FOREIGN KEY ("statusId") REFERENCES status("statusId") ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE sprint_results (
        "resultId" BIGINT PRIMARY KEY, 
        "raceId" BIGINT, 
        "driverId" BIGINT, 
        "constructorId" BIGINT, 
        number BIGINT, 
        grid BIGINT, 
        position TEXT, 
        "positionText" TEXT, 
        "positionOrder" BIGINT, 
        points BIGINT, 
        laps BIGINT, 
        time TEXT, 
        milliseconds TEXT, 
        "fastestLap" TEXT, 
        "fastestLapTime" TEXT, 
        "statusId" BIGINT,
        FOREIGN KEY ("raceId") REFERENCES races("raceId") ON DELETE CASCADE,
        FOREIGN KEY ("driverId") REFERENCES drivers("driverId") ON DELETE CASCADE,
        FOREIGN KEY ("constructorId") REFERENCES constructors("constructorId") ON DELETE CASCADE,
        FOREIGN KEY ("statusId") REFERENCES status("statusId") ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE status (
        "statusId" BIGINT PRIMARY KEY, 
        status TEXT
    );
    """
]

def create_database():
    with engine.connect() as conn:
        # Ativar as chaves estrangeiras
        conn.execute(text(SCHEMA_SQL))
        
        # Executar cada declaração CREATE TABLE individualmente
        for statement in create_statements:
            conn.execute(text(statement))

    print(f'database {DB_NAME} has been created.')

if __name__ == '__main__':
    create_database()