from typing import Final

GET_CARRIER_BY_ID_QUERY: Final[str] = "SELECT * FROM c WHERE c.id = @id"

CARRIER_PRICE_QUERY: Final[
    str
] = """
SELECT 
  Carrier.id, 
  Carrier.name, 
  ARRAY(
    SELECT 
      * 
    FROM 
      p IN Carrier.prices 
    WHERE 
      p.origin.zip_code = @origin_zip_code 
      AND p.destination.zip_code = @destination_zip_code
  ) AS prices 
FROM 
  Carrier 
WHERE 
  ARRAY_LENGTH(
    ARRAY(
      SELECT 
        * 
      FROM 
        p IN Carrier.prices 
      WHERE 
        p.origin.zip_code = @origin_zip_code 
        AND p.destination.zip_code = @destination_zip_code
    )
  ) > 0
"""
