import pyarrow.parquet as pq
from io import BytesIO


class DataFrameSerializer:
    """
    Serialize/De-serialize Pandas DataFrame to/from Bytes
    """
    @staticmethod
    def serialize(df):
        if df is None:
            return bytes()
        else:
            buffer = BytesIO()
            df.to_parquet(buffer, engine='pyarrow')
            return buffer.getvalue()

    @staticmethod
    def deserialize(s_bytes):
        if len(s_bytes) != 0:
            return pq.read_table(BytesIO(s_bytes)).to_pandas()
        else:
            return None
