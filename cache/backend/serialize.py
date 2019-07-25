import pyarrow.parquet as pq
from io import BytesIO


class DataFrameSerializer:
    """
    Serialize/De-serialize Pandas DataFrame to/from Bytes using PyArrow
    Note that Parquet format doesn't allow mixed data types for a column
    """
    @staticmethod
    def serialize(df):
        """
        Serialize a Pandas DataFrame through Apache Parquet format
        :param df: Pandas DataFrame or None
        :return: bytes
        """
        if df is None:
            return bytes()
        else:
            buffer = BytesIO()
            df.to_parquet(buffer, engine='pyarrow')
            return buffer.getvalue()

    @staticmethod
    def deserialize(s_bytes):
        """
        Deserialize Apache Parquet to Pandas DataFrame
        :param s_bytes: bytes representation of a Apache Parquet
        :return: Pandas DataFrame or None
        """
        if len(s_bytes) != 0:
            return pq.read_table(BytesIO(s_bytes)).to_pandas()
        else:
            return None
