import unittest
import pandas as pd

from unittest.mock import patch
from src.utils.s3 import s3_athena_load_table_parquet_snappy


class TestS3AthenaLoadTable(unittest.TestCase):

    @patch("src.utils.s3.wr.s3.to_parquet")
    def test_upload_is_called_with_expected_arguments(self, mock_to_parquet):
        df = pd.DataFrame({
            "code": ["A01", "B02"],
            "description": ["Desc A", "Desc B"],
            "year": ["2025", "2025"],
            "code_type": ["CM", "CM"]
        })
        
        s3_athena_load_table_parquet_snappy(
            df=df,
            database="my_database",
            table_name="my_table",
            table_location="s3://bucket/path/",
            s3_file_prefix="20250514_",
            insert_mode="overwrite",
            partition_cols=["code_type", "year"]
        )

        mock_to_parquet.assert_called_once()

        args, kwargs = mock_to_parquet.call_args
        self.assertEqual(kwargs["database"], "my_database")
        self.assertEqual(kwargs["table"], "my_table")
        self.assertEqual(kwargs["compression"], "snappy")
        self.assertEqual(kwargs["partition_cols"], ["code_type", "year"])


    @patch("src.utils.s3.wr.s3.to_parquet")
    def test_upload_is_skipped_for_empty_dataframe(self, mock_to_parquet):
        df = pd.DataFrame(columns=["code", "description", "year", "code_type"])

        s3_athena_load_table_parquet_snappy(
            df=df,
            database="my_database",
            table_name="my_table",
            table_location="s3://bucket/path/",
            partition_cols=["code_type", "year"]

        )

        mock_to_parquet.assert_not_called()


if __name__ == "__main__":
    unittest.main()
