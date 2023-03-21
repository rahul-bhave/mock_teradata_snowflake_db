import unittest
from unittest.mock import MagicMock
from unittest.mock import patch
from compare.compare_tables import compare_tables, connect_to_teradata, connect_to_snowflake
import pandas as pd
import numpy as np
import logging
from pythonjsonlogger import jsonlogger
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# logging
log_handler = logging.StreamHandler()
log_handler.setFormatter(jsonlogger.JsonFormatter())
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

class TestConnectToTeradata(unittest.TestCase):
    def setUp(self):
        self.config = {
            'teradata': {
                'host': 'localhost',
                'user': 'user',
                'password': 'password'
            }
        }

    @patch('compare.compare_tables.teradatasql.connect')
    def test_connect_to_teradata(self, mock_connect):
        # Set up mock data
        mock_connect.return_value = 'Teradata connection'

        # Call the function
        conn = connect_to_teradata(self.config)

        # Check the results
        self.assertEqual(conn, 'Teradata connection')
        mock_connect.assert_called_once_with(
            host=self.config['teradata']['host'],
            user=self.config['teradata']['user'],
            password=self.config['teradata']['password']
        )

class TestConnectToSnowflake(unittest.TestCase):
    def setUp(self):
        self.config = {
            'snowflake': {
                'user': 'user',
                'password': 'password',
                'account': 'account',
                'warehouse': 'warehouse',
                'database': 'database',
                'schema': 'schema'
            }
        }
    
    @patch('compare.compare_tables.snowflake.connector.connect')
    def test_connect_to_snowflake(self, mock_connect):
        # Set up mock data
        mock_connect.return_value = 'Snowflake connection'

        # Call the function
        conn = connect_to_snowflake(self.config)

        # Check the results
        self.assertEqual(conn, 'Snowflake connection')
        mock_connect.assert_called_once_with(
            user=self.config['snowflake']['user'],
            password=self.config['snowflake']['password'],
            account=self.config['snowflake']['account'],
            warehouse=self.config['snowflake']['warehouse'],
            database=self.config['snowflake']['database'],
            schema=self.config['snowflake']['schema']
        )

    @patch('compare.compare_tables.snowflake.connector.connect')
    def test_connect_to_snowflake(self, mock_connect):
        # Set up mock data
        mock_connect.return_value = 'Snowflake connection'

        # Call the function
        conn = connect_to_snowflake(self.config)

        # Check the results
        self.assertEqual(conn, 'Snowflake connection')
        mock_connect.assert_called_once_with(
            user=self.config['snowflake']['user'],
            password=self.config['snowflake']['password'],
            account=self.config['snowflake']['account'],
            warehouse=self.config['snowflake']['warehouse'],
            database=self.config['snowflake']['database'],
            schema=self.config['snowflake']['schema']
        )

class TestCompareTables(unittest.TestCase):
    def setUp(self):
        self.config = {
            'teradata': {
                'host': 'localhost',
                'user': 'user',
                'password': 'password'
            },
            'snowflake': {
                'user': 'user',
                'password': 'password',
                'account': 'account',
                'warehouse': 'warehouse',
                'database': 'database',
                'schema': 'schema'
            }
        }

  
    @patch('compare.compare_tables.pd.read_sql')
    def test_compare_tables(self, mock_read_sql):
        # Set up mock data
        td_query = "SELECT * FROM table"
        sf_query = "SELECT * FROM table"
        df_td = pd.DataFrame(columns=['col1', 'col2'], data=[[1, 'a'], [2, 'b'], [3, 'c']])
        df_sf = pd.DataFrame(columns=['col1', 'col2'], data=[[1, 'a'], [2, 'b'], [4, 'd']])
        mock_read_sql.side_effect = [df_td, df_sf]

        # Call the function
        conn_td = MagicMock()
        conn_sf = MagicMock()
        results = compare_tables(self.config, conn_td, conn_sf, 'table')

        # Check the results
        expected_results = pd.DataFrame(columns=['col1', 'col2'], data=[[3, 'c'], [4, 'd']])
        np.testing.assert_array_equal(results.values, expected_results.values)


if __name__ == '__main__':
    unittest.main()
