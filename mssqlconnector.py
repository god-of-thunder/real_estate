from dataclasses import dataclass, field
import pyodbc
import pandas as pd


@dataclass
class MssqlConnector:
    """base mssql connector"""

    host: str
    user: str
    password: str
    db: str
    _connect: pyodbc.Connection = field(init=False)
    _cursor: pyodbc.Cursor = field(init=False)
    
    def __post_init__(self):
        self._connect = pyodbc.connect(
           "DRIVER={ODBC Driver 18 for SQL Server};"+f"SERVER={self.host};PORT=1433;DATABASE={self.db};UID={self.user};PWD={self.password};Trusted_Connection=no;Encrypt=yes;TrustServerCertificate=yes;" 
        )
        self._cursor = self._connect.cursor()

    def close_connect(self):
        """close connector"""

        self._connect.close()

    def close_cursor(self):
        """close cursor"""

        self._cursor.close()

    def close(self):
        """close"""
        self.close_cursor()
        self.close_connect()

class SQL(MssqlConnector):
    
    def select(self, sql: str) -> tuple:
        self._cursor.execute(sql)
        result = self._cursor.fetchall()
        return result

    def query(self, sql: str) -> int:
        self._cursor.execute(sql)
        self._cursor.commit()
        result = 0
        return result
    
    def insert_df(self, df: pd.DataFrame) -> int:
        df = df.to_records(index=False).tolist()
        self._cursor.executemany(sql, df)
        self._connector.commit()
        result = 0
        return result


